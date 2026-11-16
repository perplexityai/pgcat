//! Rewrites server responses for statements a plugin planned.
//!
//! Normally the server's replies are forwarded to the client as they arrive.
//! When a batch contains a planned statement, the replies are taken apart into
//! whole messages and matched to the Describe, Execute or Query message that
//! produced them, so the plugin's hooks can rewrite descriptions and rows.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use log::error;
use parking_lot::Mutex;

use crate::errors::Error;
use crate::messages::{
    column_formats, error_message, parameter_description, parameter_types, split_messages, DataRow,
    RowDescription,
};
use crate::plugins::hook_errors::Hook;
use crate::plugins::{run_hook, ExecuteContext, RowsContext, StatementPlan};

/// A portal bound to a planned statement.
pub struct PortalPlan {
    pub plan: Arc<StatementPlan>,

    /// Result format codes of the Bind, as sent to the server.
    pub result_formats: Vec<i16>,

    /// The client's `authorization` count when it was bound.
    pub authorization: u64,

    /// The server's description of the portal, if it was described.
    described: Mutex<Option<RowDescription>>,
}

impl PortalPlan {
    pub fn new(
        plan: Arc<StatementPlan>,
        result_formats: Vec<i16>,
        authorization: u64,
    ) -> PortalPlan {
        PortalPlan {
            plan,
            result_formats,
            authorization,
            described: Mutex::new(None),
        }
    }
}

/// A message sent to the server whose reply hasn't been seen in full yet.
enum Pending {
    /// Describe of a statement: ParameterDescription, then RowDescription or NoData.
    DescribeStatement(Option<Arc<StatementPlan>>),

    /// Describe of a portal: RowDescription or NoData.
    DescribePortal(Option<Arc<PortalPlan>>),

    /// Describe of a portal pgcat sent itself (`describe_for_rows`): its reply
    /// is the portal's, and the client doesn't get it.
    HiddenDescribe(Arc<PortalPlan>),

    /// Execute: DataRows, then CommandComplete, PortalSuspended or EmptyQueryResponse.
    Execute(Option<Arc<PortalPlan>>),

    /// A simple Query: any number of result sets, until ReadyForQuery.
    Query(Arc<StatementPlan>),
}

impl Pending {
    fn plan(&self) -> Option<&Arc<StatementPlan>> {
        match self {
            Pending::DescribeStatement(plan) => plan.as_ref(),
            Pending::DescribePortal(portal) | Pending::Execute(portal) => {
                portal.as_ref().map(|portal| &portal.plan)
            }
            Pending::HiddenDescribe(portal) => Some(&portal.plan),
            Pending::Query(plan) => Some(plan),
        }
    }

    /// Rows produced by this message go through the plugin.
    fn rewrites_rows(&self) -> bool {
        match self {
            Pending::Execute(Some(portal)) => portal.plan.wants_response,
            Pending::Query(plan) => plan.wants_response,
            _ => false,
        }
    }
}

/// Follows the server's replies for one batch (up to ReadyForQuery).
#[derive(Default)]
pub struct ResponseRewriter {
    pending: VecDeque<Pending>,

    /// The current batch needs rewriting.
    active: bool,

    /// Description of the current result set of a simple Query.
    query_description: Option<RowDescription>,

    /// Rows waiting for the plugin.
    rows: Vec<DataRow>,
    rows_bytes: usize,

    /// A hook failed and the client got an error: drop everything until ReadyForQuery.
    failed: bool,
    /// A hook failed since `take_outcome` was last called.
    batch_failed: bool,
    /// pgcat opened a transaction for the batch (`needs_transaction`).
    wrapped: bool,
    /// ... and the batch began one itself: the transaction is the client's.
    client_began: bool,
}

impl ResponseRewriter {
    /// A Describe of a statement was sent to the server.
    pub fn describe_statement(&mut self, plan: Option<Arc<StatementPlan>>) {
        self.pending.push_back(Pending::DescribeStatement(plan));
    }

    /// A Describe of a portal was sent to the server.
    pub fn describe_portal(&mut self, portal: Option<Arc<PortalPlan>>) {
        self.pending.push_back(Pending::DescribePortal(portal));
    }

    /// pgcat describes a portal of a statement whose rows a plugin reads,
    /// right after its Bind: the rows must be read by this execution's
    /// description, as the statement's may predate what its tables resolve to
    /// now (another `search_path`, another shard). Returns the Describe to send.
    pub fn describe_for_rows(&mut self, portal_name: &str, portal: Arc<PortalPlan>) -> BytesMut {
        self.pending.push_back(Pending::HiddenDescribe(portal));
        let mut describe = BytesMut::with_capacity(7 + portal_name.len());
        describe.put_u8(b'D');
        describe.put_i32((4 + 1 + portal_name.len() + 1) as i32);
        describe.put_u8(b'P');
        describe.put_slice(portal_name.as_bytes());
        describe.put_u8(0);
        describe
    }

    /// An Execute was sent to the server.
    pub fn execute(&mut self, portal: Option<Arc<PortalPlan>>) {
        self.pending.push_back(Pending::Execute(portal));
    }

    /// A simple Query was sent to the server.
    pub fn query(&mut self, plan: Arc<StatementPlan>) {
        self.pending.push_back(Pending::Query(plan));
    }

    /// The batch has been sent. Returns true if its replies need rewriting;
    /// otherwise they can be forwarded as-is and nothing is tracked.
    pub fn start(&mut self) -> bool {
        self.active = self.pending.iter().any(|pending| {
            pending
                .plan()
                .map(|plan| plan.rewrites_response())
                .unwrap_or(false)
        });

        if !self.active {
            self.reset();
        }

        self.active
    }

    pub fn active(&self) -> bool {
        self.active
    }

    /// After `start`: whether a hook failing could leave writes behind, unless
    /// the batch runs in a transaction pgcat rolls back. Only plans with response
    /// hooks (`wants_response`) have hooks that can fail; with one, the batch
    /// needs it unless it runs a single statement that only reads.
    pub fn needs_transaction(&self) -> bool {
        let hooked = self
            .pending
            .iter()
            .any(|pending| pending.plan().map_or(false, |plan| plan.wants_response));
        if !self.active || !hooked {
            return false;
        }

        let mut statements = 0;
        let mut read_only = true;
        for pending in &self.pending {
            match pending {
                Pending::Execute(portal) => {
                    statements += 1;
                    read_only &= portal
                        .as_ref()
                        .map_or(false, |portal| portal.plan.read_only);
                }
                Pending::Query(plan) => {
                    statements += 1;
                    read_only &= plan.read_only;
                }
                _ => (),
            }
        }

        statements > 1 || !read_only
    }

    /// pgcat opened a transaction for the batch.
    pub fn wrapped(&mut self) {
        self.wrapped = true;
    }

    /// Whether a hook failed since the last call, and whether the batch began a
    /// transaction of its own inside the one pgcat opened for it.
    pub fn take_outcome(&mut self) -> (bool, bool) {
        let outcome = (self.batch_failed, self.client_began);
        self.batch_failed = false;
        self.wrapped = false;
        self.client_began = false;
        outcome
    }

    /// Forget the current batch.
    pub fn reset(&mut self) {
        self.pending.clear();
        self.active = false;
        self.query_description = None;
        self.rows.clear();
        self.rows_bytes = 0;
        self.failed = false;
    }

    /// Rewrite a chunk of the server's reply. Chunks are whole messages. Rows may be
    /// held back until there are enough for a batch or the result ends.
    ///
    /// Hook errors are sent to the client as an ErrorResponse. An `Err` means the
    /// reply didn't match the batch and the server connection can't be trusted.
    pub async fn process(
        &mut self,
        response: BytesMut,
        ctx: &mut ExecuteContext<'_>,
    ) -> Result<BytesMut, Error> {
        let mut out = BytesMut::with_capacity(response.len());

        for message in split_messages(&response)? {
            let code = message[0];

            if code != b'D' && !self.failed {
                self.flush_rows(ctx, &mut out).await?;
            }

            if self.failed {
                if code == b'Z' {
                    self.reset();
                    out.put(message);
                }
                continue;
            }

            match code {
                // ParameterDescription
                b't' => match self.pending.front() {
                    Some(Pending::DescribeStatement(plan)) => match plan {
                        Some(plan) if plan.rewrites_response() => {
                            let mut types = parameter_types(&message)?;
                            match plan.hooks.on_param_description(plan, &mut types) {
                                Ok(()) => out.put(parameter_description(&types)),
                                Err(err) => {
                                    let plan = plan.clone();
                                    self.fail(&plan, Hook::ParamDescription, err, &mut out)
                                }
                            }
                        }
                        _ => out.put(message),
                    },
                    _ => return Err(unexpected(code)),
                },

                // RowDescription
                b'T' => {
                    let plan = match self.pending.front() {
                        Some(Pending::DescribeStatement(plan)) => {
                            let plan = plan.clone();
                            if let Some(ref plan) = plan {
                                *plan.described.lock() = Some(message[..].try_into()?);
                            }
                            self.pending.pop_front();
                            plan
                        }

                        Some(Pending::HiddenDescribe(portal)) => {
                            *portal.described.lock() = Some(message[..].try_into()?);
                            self.pending.pop_front();
                            continue;
                        }

                        Some(Pending::DescribePortal(portal)) => {
                            let plan = portal.as_ref().map(|portal| portal.plan.clone());
                            if let Some(ref portal) = portal {
                                *portal.described.lock() = Some(message[..].try_into()?);
                            }
                            self.pending.pop_front();
                            plan
                        }

                        Some(Pending::Query(plan)) => {
                            let plan = plan.clone();
                            self.query_description = Some(message[..].try_into()?);
                            Some(plan)
                        }

                        _ => return Err(unexpected(code)),
                    };

                    match plan {
                        Some(plan) if plan.wants_response => {
                            let mut description: RowDescription = message[..].try_into()?;
                            match plan.hooks.on_row_description(&plan, &mut description) {
                                Ok(()) => out.put(BytesMut::from(&description)),
                                Err(err) => self.fail(&plan, Hook::RowDescription, err, &mut out),
                            }
                        }
                        _ => out.put(message),
                    }
                }

                // NoData
                b'n' => match self.pending.front() {
                    Some(Pending::DescribeStatement(_)) | Some(Pending::DescribePortal(_)) => {
                        self.pending.pop_front();
                        out.put(message);
                    }
                    Some(Pending::HiddenDescribe(_)) => {
                        self.pending.pop_front();
                    }
                    _ => return Err(unexpected(code)),
                },

                // DataRow
                b'D' => match self.pending.front() {
                    Some(pending) if pending.rewrites_rows() => {
                        self.rows_bytes += message.len();
                        self.rows.push(message[..].try_into()?);

                        if self.rows.len() >= ctx.pool_settings.plugin_response_batch_rows.max(1)
                            || self.rows_bytes >= ctx.pool_settings.plugin_response_batch_bytes
                        {
                            self.flush_rows(ctx, &mut out).await?;
                        }
                    }
                    Some(Pending::Execute(_)) | Some(Pending::Query(_)) => out.put(message),
                    _ => return Err(unexpected(code)),
                },

                // CommandComplete, PortalSuspended, EmptyQueryResponse
                b'C' | b's' | b'I' => {
                    match self.pending.front() {
                        Some(Pending::Execute(_)) => {
                            self.pending.pop_front();
                        }
                        Some(Pending::Query(_)) => self.query_description = None,
                        _ => return Err(unexpected(code)),
                    }
                    out.put(message);
                }

                // ErrorResponse: the server skips the rest of the batch.
                b'E' => {
                    self.pending.clear();
                    self.query_description = None;
                    out.put(message);
                }

                // ReadyForQuery: the batch is over.
                b'Z' => {
                    self.reset();
                    out.put(message);
                }

                // COPY can't be followed.
                b'G' | b'H' | b'W' => return Err(unexpected(code)),

                // In a transaction pgcat opened, the batch's own BEGIN only gets a
                // warning (25001). The transaction is then the client's: keep it
                // open, and keep the warning from the client, who didn't cause it.
                b'N' if self.wrapped && notice_code(&message) == Some("25001") => {
                    self.client_began = true;
                }

                // ParseComplete, BindComplete, CloseComplete, notices, parameter status...
                _ => out.put(message),
            }
        }

        Ok(out)
    }

    /// Send the rows waiting for the plugin through its hook.
    async fn flush_rows(
        &mut self,
        ctx: &mut ExecuteContext<'_>,
        out: &mut BytesMut,
    ) -> Result<(), Error> {
        if self.rows.is_empty() {
            return Ok(());
        }

        let rows = std::mem::take(&mut self.rows);
        self.rows_bytes = 0;

        let (plan, result_formats, description) = match self.pending.front() {
            Some(Pending::Execute(Some(portal))) => {
                let description = portal
                    .described
                    .lock()
                    .clone()
                    .or_else(|| portal.plan.described.lock().clone());
                (portal.plan.clone(), &portal.result_formats[..], description)
            }
            Some(Pending::Query(plan)) => (plan.clone(), &[][..], self.query_description.clone()),
            _ => return Err(unexpected(b'D')),
        };

        // A hook reads a row by its description: one that doesn't match it
        // can't be rewritten, nor forwarded unread.
        let columns = description
            .as_ref()
            .map_or(rows[0].values.len(), |description| description.fields.len());
        if let Some(row) = rows.iter().find(|row| row.values.len() != columns) {
            return Err(Error::ProtocolSyncError(format!(
                "a row of {} columns where the description has {}",
                row.values.len(),
                columns
            )));
        }
        let formats = column_formats(result_formats, columns);

        let rows_ctx = RowsContext {
            formats: &formats,
            description: description.as_ref(),
        };

        let timeout = ctx.pool_settings.plugin_hook_timeout;
        match run_hook(timeout, plan.hooks.on_rows(ctx, &plan, &rows_ctx, rows)).await {
            Ok(rows) => {
                for row in &rows {
                    out.put(BytesMut::from(row));
                }
            }
            Err(err) => self.fail(&plan, Hook::Rows, err, out),
        }

        Ok(())
    }

    /// `plan`'s `hook` failed: tell the client, counted for the plugin that
    /// planned it, and drop the rest of the reply until ReadyForQuery.
    fn fail(&mut self, plan: &StatementPlan, hook: Hook, err: Error, out: &mut BytesMut) {
        error!("Plugin response hook failed: {}", err);
        out.put(error_message(&plan.hook_failed(hook, &err)));
        self.failed = true;
        self.batch_failed = true;
        self.rows.clear();
        self.rows_bytes = 0;
    }
}

/// The SQLSTATE of a NoticeResponse.
fn notice_code(message: &[u8]) -> Option<&str> {
    let mut fields = message.get(5..)?.split(|byte| *byte == 0);
    fields.find_map(|field| match field.split_first() {
        Some((b'C', code)) => std::str::from_utf8(code).ok(),
        _ => None,
    })
}

fn unexpected(code: u8) -> Error {
    Error::ProtocolSyncError(format!(
        "unexpected '{}' while rewriting the server's response",
        code as char
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{
        bind_complete, command_complete, data_row, parse_complete, ready_for_query,
        row_description, with_sqlstate, DataType,
    };
    use crate::plugins::{hook_errors, StatementHooks};
    use crate::pool::PoolSettings;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Drops the last column and counts its calls. Fails on rows starting with "fail".
    #[derive(Default)]
    struct DropLast {
        batches: AtomicUsize,
    }

    #[async_trait]
    impl StatementHooks for DropLast {
        fn on_row_description(
            &self,
            _plan: &StatementPlan,
            description: &mut RowDescription,
        ) -> Result<(), Error> {
            description.fields.pop();
            Ok(())
        }

        async fn on_rows(
            &self,
            _ctx: &mut ExecuteContext<'_>,
            _plan: &StatementPlan,
            rows_ctx: &RowsContext<'_>,
            mut rows: Vec<DataRow>,
        ) -> Result<Vec<DataRow>, Error> {
            self.batches.fetch_add(1, Ordering::Relaxed);
            assert_eq!(rows_ctx.formats.len(), 2);

            for row in rows.iter_mut() {
                if row.values[0].as_deref() == Some(b"fail") {
                    return Err(Error::PluginError("no".into()));
                }
                row.values.pop();
            }
            Ok(rows)
        }
    }

    /// Lets every row through, whatever its shape.
    struct PassRows;

    #[async_trait]
    impl StatementHooks for PassRows {
        async fn on_rows(
            &self,
            _ctx: &mut ExecuteContext<'_>,
            _plan: &StatementPlan,
            _rows_ctx: &RowsContext<'_>,
            rows: Vec<DataRow>,
        ) -> Result<Vec<DataRow>, Error> {
            Ok(rows)
        }
    }

    fn plan(hooks: Arc<dyn StatementHooks>) -> Arc<StatementPlan> {
        Arc::new(StatementPlan::new(hooks, Box::new(())).with_response_hooks())
    }

    fn row(a: &str, b: &str) -> BytesMut {
        data_row(&vec![a.to_string(), b.to_string()])
    }

    fn two_columns() -> BytesMut {
        row_description(&vec![("a", DataType::Text), ("b", DataType::Text)])
    }

    fn settings(batch_rows: usize) -> PoolSettings {
        PoolSettings {
            plugin_response_batch_rows: batch_rows,
            ..Default::default()
        }
    }

    fn concat(messages: &[BytesMut]) -> BytesMut {
        let mut buf = BytesMut::new();
        for message in messages {
            buf.put(&message[..]);
        }
        buf
    }

    #[tokio::test]
    async fn test_rewrites_planned_portal() {
        let hooks = Arc::new(DropLast::default());
        let plan = plan(hooks.clone());
        let portal = Arc::new(PortalPlan::new(plan, vec![], 0));

        let mut rewriter = ResponseRewriter::default();
        rewriter.describe_portal(Some(portal.clone()));
        rewriter.execute(Some(portal));
        assert!(rewriter.start());

        let settings = settings(500);
        let mut ctx = ExecuteContext::new(&settings, None);

        let response = concat(&[
            parse_complete(),
            bind_complete(),
            two_columns(),
            row("1", "x"),
            row("2", "y"),
            command_complete("SELECT 2"),
            ready_for_query(false),
        ]);

        let out = rewriter.process(response, &mut ctx).await.unwrap();

        let one_column = |a: &str| data_row(&vec![a.to_string()]);
        let mut description: RowDescription = two_columns()[..].try_into().unwrap();
        description.fields.pop();

        assert_eq!(
            out,
            concat(&[
                parse_complete(),
                bind_complete(),
                BytesMut::from(&description),
                one_column("1"),
                one_column("2"),
                command_complete("SELECT 2"),
                ready_for_query(false),
            ])
        );
        assert_eq!(hooks.batches.load(Ordering::Relaxed), 1);
        assert!(!rewriter.active());
    }

    #[tokio::test]
    async fn test_rows_are_read_by_the_portals_own_description() {
        // The statement was described when it was prepared; the portal is
        // described again by pgcat, and only that reply describes the rows.
        let hooks = Arc::new(DropLast::default());
        let plan = plan(hooks.clone());
        *plan.described.lock() = Some(
            row_description(&vec![("stale", DataType::Text)])[..]
                .try_into()
                .unwrap(),
        );
        let portal = Arc::new(PortalPlan::new(plan, vec![], 0));

        let mut rewriter = ResponseRewriter::default();
        let describe = rewriter.describe_for_rows("p", portal.clone());
        assert_eq!(&describe[..], b"D\0\0\0\x07Pp\0");
        rewriter.execute(Some(portal.clone()));
        assert!(rewriter.start());

        let settings = settings(500);
        let mut ctx = ExecuteContext::new(&settings, None);
        let response = concat(&[
            bind_complete(),
            two_columns(),
            row("1", "x"),
            command_complete("SELECT 1"),
            ready_for_query(false),
        ]);
        let out = rewriter.process(response, &mut ctx).await.unwrap();

        assert_eq!(
            out,
            concat(&[
                bind_complete(),
                data_row(&vec!["1".to_string()]),
                command_complete("SELECT 1"),
                ready_for_query(false),
            ])
        );
        let described = portal.described.lock().clone().unwrap();
        assert_eq!(described.fields.len(), 2);
    }

    #[tokio::test]
    async fn test_batches_rows_across_chunks() {
        let hooks = Arc::new(DropLast::default());
        let portal = Arc::new(PortalPlan::new(plan(hooks.clone()), vec![], 0));

        let mut rewriter = ResponseRewriter::default();
        // An unplanned Execute before the planned one is left alone.
        rewriter.execute(None);
        rewriter.execute(Some(portal));
        assert!(rewriter.start());

        let settings = settings(4);
        let mut ctx = ExecuteContext::new(&settings, None);

        let mut first = vec![row("u", "v"), command_complete("SELECT 1")];
        first.extend((0..6).map(|i| row(&i.to_string(), "x")));
        let out = rewriter.process(concat(&first), &mut ctx).await.unwrap();

        // The unplanned row went through untouched, and one batch of 4 is out.
        let rows = split_messages(&out).unwrap();
        assert_eq!(rows[0], row("u", "v"));
        assert_eq!(rows.len(), 2 + 4);
        assert_eq!(hooks.batches.load(Ordering::Relaxed), 1);

        let mut second: Vec<BytesMut> = (6..10).map(|i| row(&i.to_string(), "x")).collect();
        second.push(command_complete("SELECT 10"));
        second.push(ready_for_query(false));
        let out = rewriter.process(concat(&second), &mut ctx).await.unwrap();

        // 10 rows in batches of 4: 4, 4, 2.
        assert_eq!(hooks.batches.load(Ordering::Relaxed), 3);
        let messages = split_messages(&out).unwrap();
        assert_eq!(messages.len(), 6 + 2);
        assert!(messages[..6]
            .iter()
            .all(|message| DataRow::try_from(&message[..]).unwrap().values.len() == 1));
        assert_eq!(messages[6], command_complete("SELECT 10"));
    }

    /// A planned portal's reply, rewritten in the given chunks.
    async fn rewrite_in_chunks(
        hooks: Arc<dyn StatementHooks>,
        reply: &[BytesMut],
        cuts: &[usize],
    ) -> Result<BytesMut, Error> {
        let portal = Arc::new(PortalPlan::new(plan(hooks), vec![], 0));
        let mut rewriter = ResponseRewriter::default();
        rewriter.describe_portal(Some(portal.clone()));
        rewriter.execute(Some(portal));
        assert!(rewriter.start());
        let settings = settings(2);
        let mut ctx = ExecuteContext::new(&settings, None);

        let mut out = BytesMut::new();
        let mut from = 0;
        for to in cuts.iter().copied().chain([reply.len()]) {
            out.put(rewriter.process(concat(&reply[from..to]), &mut ctx).await?);
            from = to;
        }
        Ok(out)
    }

    #[tokio::test]
    async fn test_chunks_split_anywhere_between_messages() {
        let mut reply = vec![bind_complete(), two_columns()];
        reply.extend((0..5).map(|i| row(&i.to_string(), "x")));
        reply.extend([command_complete("SELECT 5"), ready_for_query(false)]);

        let hooks = Arc::new(DropLast::default());
        let whole = rewrite_in_chunks(hooks.clone(), &reply, &[]).await.unwrap();
        assert_eq!(split_messages(&whole).unwrap().len(), reply.len());
        for cut in 0..=reply.len() {
            assert_eq!(
                rewrite_in_chunks(hooks.clone(), &reply, &[cut])
                    .await
                    .unwrap(),
                whole,
                "cut after {} messages",
                cut
            );
        }
    }

    #[tokio::test]
    async fn test_row_that_does_not_match_its_description() {
        let reply = [
            bind_complete(),
            two_columns(),
            row("1", "x"),
            data_row(&vec!["2".to_string()]),
            command_complete("SELECT 2"),
            ready_for_query(false),
        ];
        let hooks = Arc::new(DropLast::default());
        let error = rewrite_in_chunks(hooks.clone(), &reply, &[])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ProtocolSyncError(_)), "{:?}", error);
        // Neither row reached the hook.
        assert_eq!(hooks.batches.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_reply_fuzz() {
        use crate::fuzz::Rng;

        fn message(code: u8, body: &[u8]) -> BytesMut {
            let mut message = BytesMut::new();
            message.put_u8(code);
            message.put_i32(body.len() as i32 + 4);
            message.put_slice(body);
            message
        }

        for seed in 0..3_000 {
            let mut rng = Rng::new(seed);
            let mut reply = vec![bind_complete(), two_columns()];
            for _ in 0..rng.below(8) {
                reply.push(match rng.below(8) {
                    0..=3 => row(&seed.to_string(), "x"),
                    4 => message(b'N', b"SNOTICE\0Mnote\0\0"),
                    5 => message(b'S', b"application_name\0x\0"),
                    6 => message(*rng.pick(b"EDTtnsICH1"), &rng.bytes(12)),
                    _ => command_complete("SELECT 1"),
                });
            }
            reply.extend([command_complete("SELECT 1"), ready_for_query(false)]);

            // A server's message mangled, with a length that matches: the
            // server's reader frames messages by their length.
            let at = rng.below(reply.len());
            let mangled = rng.mutate(&reply[at]);
            if rng.one_in(2) && mangled.len() >= 5 {
                let mut mangled = BytesMut::from(&mangled[..]);
                let len = (mangled.len() - 1) as i32;
                mangled[1..5].copy_from_slice(&len.to_be_bytes());
                reply[at] = mangled;
            }

            let mut cuts: Vec<usize> = (0..rng.below(4))
                .map(|_| rng.below(reply.len() + 1))
                .collect();
            cuts.sort();
            let whole = rewrite_in_chunks(Arc::new(PassRows), &reply, &[]).await;
            let chunked = rewrite_in_chunks(Arc::new(PassRows), &reply, &cuts).await;
            match (whole, chunked) {
                (Ok(whole), Ok(chunked)) => assert_eq!(whole, chunked, "seed {}", seed),
                (whole, chunked) => assert_eq!(whole.is_ok(), chunked.is_ok(), "seed {}", seed),
            }
        }
    }

    #[tokio::test]
    async fn test_hook_error_mid_result() {
        let hooks = Arc::new(DropLast::default());
        let portal = Arc::new(PortalPlan::new(plan(hooks.clone()), vec![1], 0));

        let mut rewriter = ResponseRewriter::default();
        rewriter.execute(Some(portal));
        rewriter.execute(None);
        assert!(rewriter.start());

        let settings = settings(2);
        let mut ctx = ExecuteContext::new(&settings, None);

        let response = concat(&[
            row("1", "x"),
            row("2", "x"),
            row("fail", "x"),
            row("4", "x"),
            command_complete("SELECT 4"),
            // The next Execute's rows are dropped too: the client already has an error.
            row("5", "x"),
            command_complete("SELECT 1"),
            ready_for_query(true),
        ]);

        let out = rewriter.process(response, &mut ctx).await.unwrap();
        let messages = split_messages(&out).unwrap();

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0][0], b'D');
        assert_eq!(messages[1][0], b'D');
        assert_eq!(messages[2][0], b'E');
        assert_eq!(messages[3], ready_for_query(true));
        assert!(!rewriter.active());
    }

    #[tokio::test]
    async fn test_portal_suspended_and_simple_query() {
        let hooks = Arc::new(DropLast::default());

        // Execute with a row limit ends with PortalSuspended.
        let portal = Arc::new(PortalPlan::new(plan(hooks.clone()), vec![], 0));
        let mut rewriter = ResponseRewriter::default();
        rewriter.execute(Some(portal));
        assert!(rewriter.start());

        let settings = settings(500);
        let mut ctx = ExecuteContext::new(&settings, None);

        let mut suspended = BytesMut::new();
        suspended.put_u8(b's');
        suspended.put_i32(4);

        let out = rewriter
            .process(
                concat(&[row("1", "x"), suspended.clone(), ready_for_query(true)]),
                &mut ctx,
            )
            .await
            .unwrap();
        assert_eq!(
            out,
            concat(&[
                data_row(&vec!["1".to_string()]),
                suspended,
                ready_for_query(true)
            ])
        );

        // A simple query with two result sets.
        rewriter.query(plan(hooks.clone()));
        assert!(rewriter.start());
        let response = concat(&[
            two_columns(),
            row("1", "x"),
            command_complete("SELECT 1"),
            two_columns(),
            row("2", "y"),
            command_complete("SELECT 1"),
            ready_for_query(false),
        ]);
        let out = rewriter.process(response, &mut ctx).await.unwrap();
        let messages = split_messages(&out).unwrap();
        assert_eq!(messages.len(), 7);
        assert_eq!(
            RowDescription::try_from(&messages[3][..])
                .unwrap()
                .fields
                .len(),
            1
        );
        assert_eq!(messages[4], data_row(&vec!["2".to_string()]));
    }

    #[tokio::test]
    async fn test_param_description_and_unexpected_messages() {
        let hooks = Arc::new(DropLast::default());
        let appended = Arc::new(
            StatementPlan::new(hooks, Box::new(()))
                .with_client_params(1)
                .with_appended_params(vec![17, 17]),
        );

        let mut rewriter = ResponseRewriter::default();
        rewriter.describe_statement(Some(appended));
        assert!(rewriter.start());

        let settings = settings(500);
        let mut ctx = ExecuteContext::new(&settings, None);

        // The appended parameters are hidden; the plan doesn't want rows, so the
        // RowDescription is left alone.
        let out = rewriter
            .process(
                concat(&[
                    parse_complete(),
                    parameter_description(&[25, 17, 17]),
                    two_columns(),
                    ready_for_query(false),
                ]),
                &mut ctx,
            )
            .await
            .unwrap();
        assert_eq!(
            out,
            concat(&[
                parse_complete(),
                parameter_description(&[25]),
                two_columns(),
                ready_for_query(false)
            ])
        );

        // Rows nobody asked for mean we lost track of the server.
        let portal = Arc::new(PortalPlan::new(
            plan(Arc::new(DropLast::default())),
            vec![],
            0,
        ));
        rewriter.describe_portal(Some(portal));
        assert!(rewriter.start());
        assert!(rewriter.process(row("1", "x"), &mut ctx).await.is_err());
    }

    /// Fails every response hook, the first with a code of its own.
    struct Fails;

    #[async_trait]
    impl StatementHooks for Fails {
        fn on_param_description(&self, _: &StatementPlan, _: &mut Vec<i32>) -> Result<(), Error> {
            Err(Error::PluginError(with_sqlstate("ENV01", "no")))
        }

        fn on_row_description(
            &self,
            _: &StatementPlan,
            _: &mut RowDescription,
        ) -> Result<(), Error> {
            Err(Error::PluginError("no".into()))
        }

        async fn on_rows(
            &self,
            _: &mut ExecuteContext<'_>,
            _: &StatementPlan,
            _: &RowsContext<'_>,
            _: Vec<DataRow>,
        ) -> Result<Vec<DataRow>, Error> {
            Err(Error::PluginError("no".into()))
        }
    }

    /// The codes of the messages `rewriter` sends the client for `response`.
    async fn codes(
        rewriter: &mut ResponseRewriter,
        ctx: &mut ExecuteContext<'_>,
        response: &[BytesMut],
    ) -> Vec<u8> {
        assert!(rewriter.start());
        let out = rewriter.process(concat(response), ctx).await.unwrap();
        split_messages(&out)
            .unwrap()
            .iter()
            .map(|message| message[0])
            .collect()
    }

    #[tokio::test]
    async fn test_each_response_hook_error_is_counted_once() {
        let mut plan = StatementPlan::new(Arc::new(Fails), Box::new(())).with_response_hooks();
        plan.plugin = "response_errors_test";
        let plan = Arc::new(plan);
        let portal = || Some(Arc::new(PortalPlan::new(plan.clone(), vec![], 0)));
        let settings = settings(500);
        let mut ctx = ExecuteContext::new(&settings, None);
        let mut rewriter = ResponseRewriter::default();

        rewriter.describe_statement(Some(plan.clone()));
        let described = [
            parameter_description(&[25]),
            two_columns(),
            ready_for_query(false),
        ];
        assert_eq!(
            codes(&mut rewriter, &mut ctx, &described).await,
            [b'E', b'Z']
        );
        rewriter.describe_portal(portal());
        let described = [two_columns(), ready_for_query(false)];
        assert_eq!(
            codes(&mut rewriter, &mut ctx, &described).await,
            [b'E', b'Z']
        );
        rewriter.execute(portal());
        let rows = [
            row("1", "x"),
            command_complete("SELECT 1"),
            ready_for_query(false),
        ];
        assert_eq!(codes(&mut rewriter, &mut ctx, &rows).await, [b'E', b'Z']);

        // Each once, for the plugin that planned the statement.
        let count = |hook, code| hook_errors::count_of("response_errors_test", hook, code);
        assert_eq!(count(Hook::ParamDescription, "ENV01"), 1);
        assert_eq!(count(Hook::RowDescription, "XX000"), 1);
        assert_eq!(count(Hook::Rows, "XX000"), 1);
    }

    #[test]
    fn test_unplanned_batch_is_not_tracked() {
        let mut rewriter = ResponseRewriter::default();
        rewriter.describe_statement(None);
        rewriter.execute(None);
        assert!(!rewriter.start());
        assert!(rewriter.pending.is_empty());
    }
}
