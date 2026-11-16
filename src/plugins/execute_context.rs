//! What statement hooks see when a batch is sent: the server connection, a
//! transaction pgcat opens for them, where a Bind is in its batch, and the
//! state a plugin keeps for one batch.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use super::server_query::server_query;
use crate::{errors::Error, identity::Identity, pool::PoolSettings, server::Server};

/// Context passed to statement hooks when the batch is sent to the server.
pub struct ExecuteContext<'a> {
    pub pool_settings: &'a PoolSettings,

    /// Caller identity set with `SET PGCAT_IDENTITY`.
    pub identity: Option<&'a Identity>,

    /// The server connection the statement will run on, before it's sent.
    server: Option<&'a mut Server>,

    /// The batch ends with a Sync, so pgcat can end a transaction it opened
    /// for it before the client sees ReadyForQuery.
    can_begin_transaction: bool,

    /// A hook called `begin_transaction` and pgcat opened one.
    began_transaction: bool,

    /// A hook's exchange with the server is under way. If the hook is cut off
    /// (it timed out) while it is, the connection is mid-exchange.
    querying: bool,

    /// What the hooks keep for the batch, while its Binds are run.
    batch: Option<&'a mut BatchState>,

    /// Where the Bind being run is in its batch.
    position: BindPosition,
}

/// Where a Bind is in its batch. Hooks run before the batch is sent, so what
/// comes before the statement, and whether it runs at all, decide what a hook
/// may rely on.
#[derive(Clone, Copy, Debug, Default)]
pub struct BindPosition {
    /// A statement before this one in the batch ends the transaction or
    /// releases its locks: COMMIT, ROLLBACK, ROLLBACK TO SAVEPOINT, one pgcat
    /// couldn't parse, like ABORT or PREPARE TRANSACTION, or a portal bound in
    /// an earlier batch, whose statement pgcat can't see. A row a hook locks
    /// now may be unlocked by the time this statement runs.
    pub locks_released: bool,
    /// The batch doesn't execute the portal this Bind makes: it runs in a
    /// later batch, if at all, after whatever ran in between.
    pub deferred: bool,
    /// A planned statement bound after this one is executed before it: it runs
    /// first, though its hook runs later, so this hook can't count it in.
    pub overtaken: bool,
}

/// What a plugin's hooks keep while pgcat runs them for one batch, e.g. what
/// the statements before this one do. Empty at the start of every batch.
#[derive(Default)]
pub struct BatchState(HashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl BatchState {
    /// The state of type `T`, made on first use.
    pub fn get<T: Default + Send + Sync + 'static>(&mut self) -> &mut T {
        self.0
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::<T>::default())
            .downcast_mut()
            .expect("kept by its type")
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<'a> ExecuteContext<'a> {
    /// A context without a server connection, e.g. while results stream in.
    pub fn new(pool_settings: &'a PoolSettings, identity: Option<&'a Identity>) -> Self {
        ExecuteContext {
            pool_settings,
            identity,
            server: None,
            can_begin_transaction: false,
            began_transaction: false,
            querying: false,
            batch: None,
            position: BindPosition::default(),
        }
    }

    /// With the state the hooks keep for the batch, for the Bind at `position`.
    pub fn with_batch(mut self, batch: &'a mut BatchState, position: BindPosition) -> Self {
        self.batch = Some(batch);
        self.position = position;
        self
    }

    /// The batch's state of type `T`, if this runs for a batch's Binds.
    pub fn batch<T: Default + Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        self.batch.as_deref_mut().map(BatchState::get)
    }

    /// See `BindPosition::locks_released`.
    pub fn earlier_releases_locks(&self) -> bool {
        self.position.locks_released
    }

    /// See `BindPosition::deferred`. False outside a batch's Binds: a simple
    /// Query runs as it's sent.
    pub fn deferred(&self) -> bool {
        self.position.deferred
    }

    /// See `BindPosition::overtaken`. False outside a batch's Binds.
    pub fn overtaken(&self) -> bool {
        self.position.overtaken
    }

    /// With the server connection the statement will run on. `can_begin_transaction`:
    /// the batch ends with a Sync (or is a simple Query).
    pub fn with_server(mut self, server: &'a mut Server, can_begin_transaction: bool) -> Self {
        self.server = Some(server);
        self.can_begin_transaction = can_begin_transaction;
        self
    }

    /// Caller identity set with `SET PGCAT_IDENTITY`. Never log its token.
    pub fn identity(&self) -> Option<&Identity> {
        self.identity
    }

    /// Statements a plugin covers must fail without an identity.
    pub fn identity_required(&self) -> bool {
        self.pool_settings.identity_required_for_plugins
    }

    /// The client is in a transaction (or pgcat opened one for this statement).
    pub fn in_transaction(&self) -> bool {
        self.server
            .as_deref()
            .map_or(false, |server| server.in_transaction())
    }

    /// Run the statement in a transaction of pgcat's own if the client isn't in
    /// one, e.g. so that a row read FOR UPDATE stays locked until the statement
    /// writes it. pgcat commits it once the client's batch is done (rolls it back
    /// if the batch failed), and the client sees the state it expects: idle.
    /// Only in `on_bind` of a batch the client ends with Sync, and in
    /// `on_execute_query`.
    pub async fn begin_transaction(&mut self) -> Result<(), Error> {
        if !self.must_begin()? {
            return Ok(());
        }
        let server = self.server.as_deref_mut().expect("checked by must_begin");

        self.querying = true;
        server.query_checked("BEGIN").await?;
        self.querying = false;
        self.began_transaction = true;
        Ok(())
    }

    /// Whether pgcat has to open a transaction: the client isn't in one. Fails
    /// where it can't (a batch ending with Flush).
    fn must_begin(&self) -> Result<bool, Error> {
        let server = self
            .server
            .as_deref()
            .ok_or_else(|| Error::PluginError("no server connection to query".into()))?;

        if server.in_transaction() {
            return Ok(false);
        }
        if !self.can_begin_transaction {
            return Err(Error::PluginError(
                "this statement needs a transaction: send it with Sync, or in a transaction".into(),
            ));
        }
        Ok(true)
    }

    /// Whether a hook had pgcat open a transaction.
    pub fn began_transaction(&self) -> bool {
        self.began_transaction
    }

    /// Whether a hook was cut off in the middle of an exchange with the
    /// server: the connection can't be used again.
    pub fn interrupted(&self) -> bool {
        self.querying
    }

    /// Run a query on the server connection the statement will run on, in the
    /// same transaction, before the statement is sent. Parameters and results
    /// are in text format. Only available in `on_bind` and `on_execute_query`.
    pub async fn query(
        &mut self,
        sql: &str,
        params: &[Option<&str>],
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let server = self
            .server
            .as_deref_mut()
            .ok_or_else(|| Error::PluginError("no server connection to query".into()))?;

        self.querying = true;
        let rows = server_query(server, sql, params, false).await;
        self.querying = false;
        rows
    }

    /// `query`, in the transaction `begin_transaction` would open first if the
    /// client isn't in one. The BEGIN goes with the query, so it costs no round
    /// trip of its own.
    pub async fn query_in_transaction(
        &mut self,
        sql: &str,
        params: &[Option<&str>],
    ) -> Result<Vec<Vec<Option<String>>>, Error> {
        let begin = self.must_begin()?;
        let server = self.server.as_deref_mut().expect("checked by must_begin");

        self.querying = true;
        let rows = server_query(server, sql, params, begin).await;
        self.querying = false;
        if begin && server.in_transaction() {
            self.began_transaction = true;
        }
        rows
    }
}
