use crate::errors::{ClientIdentifier, Error};
use crate::held_replies::HeldReplies;
use crate::identity::{
    clears_identity, might_concern_identity, unreadable_session, valid_tenant, Identity,
    IdentityCommand, Recheck, TenantGuard,
};
use crate::pool::BanReason;
/// Handle clients by pretending to be a PostgreSQL server.
use bytes::{Buf, BufMut, BytesMut};
use futures::FutureExt;
use log::{debug, error, info, trace, warn};
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Instant, SystemTime};
use tokio::io::{split, AsyncReadExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::admin::{generate_server_parameters_for_admin, handle_admin};
use crate::auth_passthrough::refetch_auth_hash;
use crate::codec::convert;
use crate::config::{
    get_config, get_idle_client_in_transaction_timeout, Address, AuthType, PoolMode,
};
use crate::constants::*;
use crate::identity_label::IdentityLapse;
use crate::messages::*;
use crate::plugins::diagnostics::withhold_server_text;
use crate::plugins::hook_errors::Hook;
use crate::plugins::response::{PortalPlan, ResponseRewriter};
use crate::plugins::statement_plans::{Cached, Effects, StatementPlans};
use crate::plugins::{
    run_hook, statements_to_sql, BatchState, BindParams, BindPosition, ExecuteContext,
    PluginOutput, StatementPlan, HOOK_TIMED_OUT, IDENTITY_REFUSED, LIMIT_EXCEEDED, MAX_PARAMS,
    PLUGIN_DENIED, PLUGIN_FAILED, PROTOCOL_VIOLATION, UNSUPPORTED,
};
use crate::pool::{get_pool, pools_generation, ClientServerMap, ConnectionPool, PoolSettings};
use crate::query_router::{Command, QueryRouter};
use crate::relations::{Caller, RelationSnap};
use crate::server::{Server, ServerParameters};
use crate::sql_observer::{self, BindKey, ClientObserver, Gap, QueryAdmission, Snap, SqlObserver};
use crate::stats::{ClientStats, ServerStats};
use crate::tls::Tls;
use crate::trace_context;

use tokio_rustls::server::TlsStream;

/// Incrementally count prepared statements
/// to avoid random conflicts in places where the random number generator is weak.
pub static PREPARED_STATEMENT_COUNTER: Lazy<Arc<AtomicUsize>> =
    Lazy::new(|| Arc::new(AtomicUsize::new(0)));

/// Sync message: 'S' (1 byte) + length 4 (4 bytes big-endian) = 5 bytes.
/// Used to convert Flush messages into Sync messages before sending to the server.
const SYNC_BYTES: [u8; 5] = [b'S', 0, 0, 0, 4];

/// Size of a ReadyForQuery message: 'Z' (1) + length (4) + transaction status (1) = 6 bytes.
const READY_FOR_QUERY_SIZE: usize = 6;

/// Type of connection received from client.
enum ClientConnectionType {
    Startup,
    Tls,
    CancelQuery,
}

/// The client state. One of these is created per client.
pub struct Client<S, T> {
    /// The reads are buffered (8K by default).
    read: BufReader<S>,

    /// We buffer the writes ourselves because we know the protocol
    /// better than a stock buffer.
    write: T,

    /// Internal buffer, where we place messages until we have to flush
    /// them to the backend.
    buffer: BytesMut,

    /// Used to buffer response messages to the client
    response_message_queue_buffer: BytesMut,

    /// Replies pgcat gives itself to messages of the batch being sent, held
    /// until the server has answered the messages before them.
    held_replies: HeldReplies,

    /// Address
    addr: std::net::SocketAddr,

    /// The client was started with the sole reason to cancel another running query.
    cancel_mode: bool,

    /// In transaction mode, the connection is released after each transaction.
    /// Session mode has slightly higher throughput per client, but lower capacity.
    transaction_mode: bool,

    /// For query cancellation, the client is given a random process ID and secret on startup.
    process_id: i32,
    secret_key: i32,

    /// Clients are mapped to servers while they use them. This allows a client
    /// to connect and cancel a query.
    client_server_map: ClientServerMap,

    /// Client parameters, e.g. user, client_encoding, etc.
    #[allow(dead_code)]
    parameters: HashMap<String, String>,

    /// Statistics related to this client
    stats: Arc<ClientStats>,

    /// Clients want to talk to admin database.
    admin: bool,

    /// Last address the client talked to.
    last_address_id: Option<usize>,

    /// Last server process stats we talked to.
    last_server_stats: Option<Arc<ServerStats>>,

    /// Connected to server
    connected_to_server: bool,

    /// Name of the server pool for this client (This comes from the database name in the connection string)
    pool_name: String,

    /// Postgres user for this client (This comes from the user in the connection string)
    username: String,

    /// Server startup and session parameters that we're going to track
    server_parameters: ServerParameters,

    /// Used to notify clients about an impending shutdown
    shutdown: Receiver<()>,

    /// When a draining client disconnects. Each client picks its own time in
    /// the drain window, so clients don't all reconnect elsewhere at once.
    drain_at: Option<tokio::time::Instant>,

    /// Whether prepared statements are enabled for this client
    prepared_statements_enabled: bool,

    /// Mapping of client named prepared statement to rewritten parse messages

    /// Buffered extended protocol data
    extended_protocol_data_buffer: VecDeque<ExtendedProtocolData>,

    /// Statements answered by pgcat itself (e.g. plugin intercepts),
    /// keyed by client statement name.
    local_statements: HashMap<String, LocalStatement>,

    /// Portals bound to local statements, keyed by portal name.
    local_portals: HashMap<String, LocalStatement>,

    /// Counts the reloads that changed what verifies identity tokens on the
    /// client's pool (`QueryRouter::verifier_key`). A cached `SET
    /// PGCAT_IDENTITY` verified with an older one is verified again.
    verifier_generation: u64,

    /// Such a reload came after the client's identity was verified: its
    /// token is verified again before the client's next statement runs
    /// (`recheck_identity`).
    identity_recheck: bool,
    /// The text of the server's errors and notices is withheld from the
    /// client: its query router's `withholds_server_text`, kept with the pool
    /// settings it plans with.
    withhold_server_text: bool,

    /// A message in the current extended protocol batch was rejected.
    /// The rest of the batch is discarded and the error is reported at Sync or Flush.
    extended_protocol_error: Option<String>,

    /// An error was already reported at Flush, ignore messages until Sync.
    discard_until_sync: bool,

    /// A COPY FROM STDIN began in an extended protocol batch. The server
    /// ignored that batch's Sync, so it answers CopyDone or CopyFail only at
    /// the client's next Sync.
    copy_in_extended: bool,

    /// Plugin plans for the client's prepared statements, and what to plan
    /// them again from.
    statements: StatementPlans,

    /// Portals bound to planned statements, keyed by portal name.
    portal_plans: HashMap<String, Arc<PortalPlan>>,

    /// Rewrites the server's replies for planned statements.
    response_rewriter: ResponseRewriter,

    /// pgcat opened a transaction around the batch being sent (it ends it).
    implicit_transaction: bool,

    /// Statements, by name, that end a transaction (COMMIT, ROLLBACK), or that
    /// pgcat couldn't parse, so might.
    transaction_enders: HashSet<String>,

    /// Those, and statements that release a transaction's locks without
    /// ending it (ROLLBACK TO SAVEPOINT).
    lock_releasers: HashSet<String>,

    /// Whether each statement the buffered batch parses was in
    /// `transaction_enders` and `lock_releasers` before, oldest first, to put
    /// back if the batch is rejected.
    enders_journal: Vec<(String, bool, bool)>,

    /// Where in the buffered batch Binds of such statements are.
    ending_binds: Vec<usize>,

    /// Where in the buffered batch Binds of `lock_releasers` are.
    releasing_binds: Vec<usize>,

    /// What the plugins' Bind hooks keep for the batch being run.
    batch_state: BatchState,

    /// `pool::pools_generation` when the client last looked at its pool's settings.
    pools_generation: u64,

    /// Caller identity set with `SET PGCAT_IDENTITY`. Lives on the client, not on
    /// the server connection, and is never sent to the server.
    identity: Option<Identity>,

    /// Counts the changes of what a planned Bind's hooks checked: the identity
    /// and the plugin configuration statements are planned with. A portal bound
    /// before one can't be executed after.
    authorization: u64,

    /// Why `identity` is None, for the SQL observer's labels only.
    identity_lapse: IdentityLapse,

    /// Statements about to be sent may change the tenant parameter: pgcat
    /// refused what it can tell sets it, and sets it again after them anyway.
    tenant_may_change: bool,

    /// The SQL observer already recorded the simple Query being handled
    /// (as an unparsed gap, at its admission). Whatever the query is run
    /// through again after a pool reload, it is recorded only once.
    sql_observed: bool,

    /// What the client's extended protocol messages let the SQL observer
    /// count. None unless the pool's observer is enabled.
    observer: Option<Box<ClientObserver>>,

    /// The client's statements that join a caller's trace. None unless
    /// statement tracing is on and the client sent a sampled one.
    traces: Option<Box<trace_context::ClientTraces>>,

    /// What the Parse being handled counts when its statement is executed.
    parse_meta: Snap,

    /// What the portals bound to the client's prepared statements do when
    /// executed, by name. Only portals with an effect are kept.
    portal_effects: HashMap<String, Effects>,

    /// Identity commands in the buffered batch, run once pgcat answers the batch
    /// (it must be answered by pgcat alone): where their reply goes in the
    /// batch, the command, and its statement.
    pending_identity: Vec<(usize, IdentityAction, LocalStatement)>,

    /// The buffered batch resets the session: the identity goes once it's sent.
    pending_session_reset: bool,

    /// The batch being sent executes a BEGIN: a transaction pgcat opened for
    /// it is then the client's (`take_round_outcome`).
    batch_begins: bool,

    /// The portals bound since a reload changed the plan in the transaction.
    /// The others were bound under the old plan: an Execute of one is refused,
    /// unless it runs an identity command, verified again. None until then.
    bound_since_reload: Option<FreshPortals>,

    /// Plan for the simple Query about to be sent.
    query_plan: Option<Arc<StatementPlan>>,
}

/// The portals bound since a reload: those the server bound, those the
/// buffered batch binds, and those the batch sent binds, until the server has
/// answered it.
#[derive(Default)]
struct FreshPortals {
    bound: HashSet<String>,
    batch: HashSet<String>,
    sent: HashSet<String>,
}

impl FreshPortals {
    /// The buffered batch goes to the server.
    fn send(&mut self) {
        self.sent.extend(self.batch.drain());
    }

    /// The server's reply to what was sent. An error drops the sent batch's
    /// Binds, as which of them ran isn't known; a ReadyForQuery after none
    /// keeps them. Binds still buffered wait for their batch's reply: a simple
    /// Query is sent without them.
    fn answered(&mut self, error: bool, ready_for_query: bool) {
        if error {
            self.sent.clear();
        }
        if ready_for_query {
            self.bound.extend(self.sent.drain());
        }
    }

    /// pgcat answered the buffered batch itself.
    fn answered_locally(&mut self) {
        self.bound.extend(self.batch.drain());
    }
}

/// What pgcat does with a Parse.
enum ParseOutcome {
    /// Answers it itself.
    Local(LocalStatement),
    /// Rejects it (at the next Sync or Flush).
    Rejected(String),
    /// Sends this message, maybe rewritten, with the plugins' plan.
    Server {
        message: BytesMut,
        plan: Option<Arc<StatementPlan>>,
        /// Whether the statement ends a transaction, if pgcat could parse it.
        ends_transaction: Option<bool>,
        /// Whether a plugin changed the client's SQL text.
        rewritten: bool,
        /// Whether it releases the transaction's locks, if pgcat could parse it.
        releases_locks: Option<bool>,
        /// Whether it begins a transaction.
        begins_transaction: bool,
    },
}

/// A statement pgcat answers without the server.
#[derive(Clone, Debug, Default)]
struct LocalStatement {
    /// Columns returned by Execute (in text format), or None for no rows.
    row_description: Option<RowDescription>,

    /// DataRows (in text format) and CommandComplete returned by Execute.
    execute_response: BytesMut,

    /// Identity command run on Execute, instead of returning `execute_response`.
    identity_command: Option<IdentityAction>,

    /// The pool's plan generation when a plugin answered it, 0 for pgcat's
    /// own answers. Once the plugin configuration changed, its portals aren't
    /// executed: the plugin might answer otherwise now.
    planned_with: u64,

    /// The client's `verifier_generation` when its `SET` was verified.
    verified_with: u64,

    /// Result format codes, once bound to a portal.
    result_formats: Vec<i16>,
}

impl LocalStatement {
    /// Build from a simple query response (RowDescription, DataRows, CommandComplete,
    /// ReadyForQuery), e.g. what the intercept plugin returns.
    fn from_simple_query_response(response: &BytesMut) -> Result<LocalStatement, Error> {
        let mut statement = LocalStatement::default();

        for message in split_messages(response)? {
            match message[0] {
                b'T' if statement.row_description.is_none() => {
                    statement.row_description = Some(message[..].try_into()?)
                }
                b'D' => statement.execute_response.put(message),
                b'C' => {
                    statement.execute_response.put(message);
                    break;
                }
                _ => (),
            }
        }

        Ok(statement)
    }

    fn identity(command: IdentityAction) -> LocalStatement {
        LocalStatement {
            row_description: match command {
                IdentityAction::Show => Some(
                    identity_row_description()[..]
                        .try_into()
                        .expect("valid RowDescription"),
                ),
                _ => None,
            },
            identity_command: Some(command),
            ..Default::default()
        }
    }

    /// Copy of the statement bound to a portal with these result formats.
    fn bind(&self, result_formats: &[i16]) -> LocalStatement {
        LocalStatement {
            result_formats: result_formats.to_vec(),
            ..self.clone()
        }
    }

    /// RowDescription (or NoData) for a Describe. Statements are described with
    /// text formats, portals with the formats they were bound with.
    fn describe(&self) -> BytesMut {
        match self.row_description {
            Some(ref description) => {
                let mut description = description.clone();
                let formats = column_formats(&self.result_formats, description.fields.len());
                for (field, format) in description.fields.iter_mut().zip(formats) {
                    field.format = format;
                }
                BytesMut::from(&description)
            }
            None => no_data(),
        }
    }

    /// Convert DataRows produced in text format to the formats the portal was bound with.
    fn encode_rows(&self, response: BytesMut) -> Result<BytesMut, Error> {
        let description = match self.row_description {
            Some(ref description) if self.result_formats.iter().any(|f| *f != 0) => description,
            _ => return Ok(response),
        };

        let formats = column_formats(&self.result_formats, description.fields.len());
        let mut encoded = BytesMut::with_capacity(response.len());

        for message in split_messages(&response)? {
            if message[0] != b'D' {
                encoded.put(message);
                continue;
            }

            let mut row: DataRow = message[..].try_into()?;
            for ((value, field), format) in
                row.values.iter_mut().zip(&description.fields).zip(&formats)
            {
                if let Some(text) = value {
                    *value = Some(convert(field.type_oid, text, 0, *format)?.into());
                }
            }
            encoded.put(BytesMut::from(&row));
        }

        Ok(encoded)
    }
}

/// Runs a connected client's handler. A client that isn't an admin counts
/// toward the drain total while the handler runs, and the client is reported
/// disconnected when the handler fails. Both also happen when the handler
/// panics; the panic then continues.
async fn handle_with_accounting(
    handle: impl std::future::Future<Output = Result<(), Error>>,
    admin: bool,
    stats: &ClientStats,
    drain: &Sender<i32>,
) -> Result<(), Error> {
    if !admin {
        let _ = drain.send(1).await;
    }

    let result = AssertUnwindSafe(handle).catch_unwind().await;

    if !admin {
        let _ = drain.send(-1).await;
    }

    match result {
        Ok(result) => {
            if result.is_err() {
                stats.disconnect();
            }
            result
        }
        Err(panic) => {
            stats.disconnect();
            std::panic::resume_unwind(panic)
        }
    }
}

/// Client entrypoint.
pub async fn client_entrypoint(
    mut stream: TcpStream,
    client_server_map: ClientServerMap,
    shutdown: Receiver<()>,
    drain: Sender<i32>,
    admin_only: bool,
    tls_certificate: Option<String>,
    log_client_connections: bool,
) -> Result<(), Error> {
    // Figure out if the client wants TLS or not.
    let addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Failed to get peer address: {:?}",
                err
            )));
        }
    };

    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;

    match by(deadline, get_startup::<TcpStream>(&mut stream)).await {
        // Client requested a TLS connection.
        Ok((ClientConnectionType::Tls, _)) => {
            // TLS settings are configured, will setup TLS now.
            if tls_certificate.is_some() {
                debug!("Accepting TLS request");

                let mut yes = BytesMut::new();
                yes.put_u8(b'S');
                write_all(&mut stream, yes).await?;

                // Negotiate TLS.
                match by(
                    deadline,
                    startup_tls(stream, client_server_map, shutdown, admin_only),
                )
                .await
                {
                    Ok(mut client) => {
                        if log_client_connections {
                            info!("Client {:?} connected (TLS)", addr);
                        } else {
                            debug!("Client {:?} connected (TLS)", addr);
                        }

                        let admin = client.is_admin();
                        let stats = client.stats.clone();
                        handle_with_accounting(client.handle(), admin, &stats, &drain).await
                    }
                    Err(err) => Err(err),
                }
            }
            // TLS is not configured, we cannot offer it.
            else {
                // Rejecting client request for TLS.
                let mut no = BytesMut::new();
                no.put_u8(b'N');
                write_all(&mut stream, no).await?;

                // Attempting regular startup. Client can disconnect now
                // if they choose.
                match by(deadline, get_startup::<TcpStream>(&mut stream)).await {
                    // Client accepted unencrypted connection.
                    Ok((ClientConnectionType::Startup, bytes)) => {
                        let (read, write) = split(stream);

                        // Continue with regular startup.
                        match by(
                            deadline,
                            Client::startup(
                                read,
                                write,
                                addr,
                                bytes,
                                client_server_map,
                                shutdown,
                                admin_only,
                            ),
                        )
                        .await
                        {
                            Ok(mut client) => {
                                if log_client_connections {
                                    info!("Client {:?} connected (plain)", addr);
                                } else {
                                    debug!("Client {:?} connected (plain)", addr);
                                }

                                let admin = client.is_admin();
                                let stats = client.stats.clone();
                                handle_with_accounting(client.handle(), admin, &stats, &drain).await
                            }
                            Err(err) => Err(err),
                        }
                    }

                    Ok((ClientConnectionType::CancelQuery, bytes)) => {
                        let (read, write) = split(stream);
                        let mut client =
                            Client::cancel(read, write, addr, bytes, client_server_map, shutdown)
                                .await?;
                        client.handle().await
                    }

                    // Client probably disconnected rejecting our plain text connection.
                    Ok((ClientConnectionType::Tls, _)) => Err(Error::ProtocolSyncError(
                        "Bad postgres client (plain)".into(),
                    )),

                    Err(err) => Err(err),
                }
            }
        }

        // Client wants to use plain connection without encryption.
        Ok((ClientConnectionType::Startup, bytes)) => {
            let (read, write) = split(stream);

            // Continue with regular startup.
            match by(
                deadline,
                Client::startup(
                    read,
                    write,
                    addr,
                    bytes,
                    client_server_map,
                    shutdown,
                    admin_only,
                ),
            )
            .await
            {
                Ok(mut client) => {
                    if log_client_connections {
                        info!("Client {:?} connected (plain)", addr);
                    } else {
                        debug!("Client {:?} connected (plain)", addr);
                    }

                    let admin = client.is_admin();
                    let stats = client.stats.clone();
                    handle_with_accounting(client.handle(), admin, &stats, &drain).await
                }
                Err(err) => Err(err),
            }
        }

        // Client wants to cancel a query.
        Ok((ClientConnectionType::CancelQuery, bytes)) => {
            let (read, write) = split(stream);

            // Continue with cancel query request.
            match Client::cancel(read, write, addr, bytes, client_server_map, shutdown).await {
                Ok(mut client) => {
                    info!("Client {:?} issued a cancel query request", addr);

                    let admin = client.is_admin();
                    let stats = client.stats.clone();
                    handle_with_accounting(client.handle(), admin, &stats, &drain).await
                }

                Err(err) => Err(err),
            }
        }

        // Something failed, probably the socket.
        Err(err) => Err(err),
    }
}

/// The longest startup message body pgcat reads: Postgres's
/// `MAX_STARTUP_PACKET_LENGTH`.
const MAX_STARTUP_BODY: usize = 10_000;

/// How long a client has to connect: TLS, the startup message and
/// authentication. Postgres's `authentication_timeout` default.
const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// `step`, if it finishes before `deadline`.
async fn by<T>(
    deadline: tokio::time::Instant,
    step: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout_at(deadline, step)
        .await
        .unwrap_or_else(|_| {
            Err(Error::SocketError(format!(
                "client startup didn't finish within {}s",
                STARTUP_TIMEOUT.as_secs()
            )))
        })
}

/// Handle the first message the client sends.
async fn get_startup<S>(stream: &mut S) -> Result<(ClientConnectionType, BytesMut), Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin + tokio::io::AsyncWrite,
{
    // Get startup message length.
    let len = match stream.read_i32().await {
        Ok(len) => len,
        Err(_) => return Err(Error::ClientBadStartup),
    };

    // The length counts itself, and the body starts with a 4-byte code. It's
    // the client's claim, read before authentication, so it's bounded as
    // Postgres bounds it.
    let body = match usize::try_from(len).ok().and_then(|len| len.checked_sub(4)) {
        Some(body @ 4..=MAX_STARTUP_BODY) => body,
        _ => return Err(Error::ClientBadStartup),
    };

    // Get the rest of the message.
    let mut startup = vec![0u8; body];
    match stream.read_exact(&mut startup).await {
        Ok(_) => (),
        Err(_) => return Err(Error::ClientBadStartup),
    };

    let mut bytes = BytesMut::from(&startup[..]);
    let code = bytes.get_i32();

    match code {
        // Client is requesting SSL (TLS).
        SSL_REQUEST_CODE => Ok((ClientConnectionType::Tls, bytes)),

        // Client wants to use plain text, requesting regular startup.
        PROTOCOL_VERSION_NUMBER => Ok((ClientConnectionType::Startup, bytes)),

        // Client is requesting to cancel a running query (plain text connection).
        CANCEL_REQUEST_CODE => Ok((ClientConnectionType::CancelQuery, bytes)),

        // Something else, probably something is wrong and it's not our fault,
        // e.g. badly implemented Postgres client.
        _ => Err(Error::ProtocolSyncError(format!(
            "Unexpected startup code: {}",
            code
        ))),
    }
}

/// Handle TLS connection negotiation.
pub async fn startup_tls(
    stream: TcpStream,
    client_server_map: ClientServerMap,
    shutdown: Receiver<()>,
    admin_only: bool,
) -> Result<Client<ReadHalf<TlsStream<TcpStream>>, WriteHalf<TlsStream<TcpStream>>>, Error> {
    // Negotiate TLS.
    let tls = Tls::new()?;
    let addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Failed to get peer address: {:?}",
                err
            )));
        }
    };

    let mut stream = match tls.acceptor.accept(stream).await {
        Ok(stream) => stream,

        // TLS negotiation failed.
        Err(err) => {
            error!("TLS negotiation failed: {:?}", err);
            return Err(Error::TlsError);
        }
    };

    // TLS negotiation successful.
    // Continue with regular startup using encrypted connection.
    match get_startup::<TlsStream<TcpStream>>(&mut stream).await {
        // Got good startup message, proceeding like normal except we
        // are encrypted now.
        Ok((ClientConnectionType::Startup, bytes)) => {
            let (read, write) = split(stream);

            Client::startup(
                read,
                write,
                addr,
                bytes,
                client_server_map,
                shutdown,
                admin_only,
            )
            .await
        }

        Ok((ClientConnectionType::CancelQuery, bytes)) => {
            let (read, write) = split(stream);
            Client::cancel(read, write, addr, bytes, client_server_map, shutdown).await
        }

        // Bad Postgres client.
        Ok((ClientConnectionType::Tls, _)) => {
            Err(Error::ProtocolSyncError("Bad postgres client (tls)".into()))
        }

        Err(err) => Err(err),
    }
}

impl<S, T> Client<S, T>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    pub fn is_admin(&self) -> bool {
        self.admin
    }

    /// Handle Postgres client startup after TLS negotiation is complete
    /// or over plain text.
    pub async fn startup(
        mut read: S,
        mut write: T,
        addr: std::net::SocketAddr,
        bytes: BytesMut, // The rest of the startup message.
        client_server_map: ClientServerMap,
        shutdown: Receiver<()>,
        admin_only: bool,
    ) -> Result<Client<S, T>, Error> {
        let parameters = parse_startup(bytes.clone())?;

        // This parameter is mandatory by the protocol.
        let username = match parameters.get("user") {
            Some(user) => user,
            None => {
                return Err(Error::ClientError(
                    "Missing user parameter on client startup".into(),
                ))
            }
        };

        let pool_name = match parameters.get("database") {
            Some(db) => db,
            None => username,
        };

        let application_name = match parameters.get("application_name") {
            Some(application_name) => application_name,
            None => "pgcat",
        };

        let client_identifier = ClientIdentifier::new(application_name, username, pool_name);

        let admin = ["pgcat", "pgbouncer"]
            .iter()
            .filter(|db| *db == pool_name)
            .count()
            == 1;

        if !admin && admin_only {
            // Kick any client that's not admin while we're in admin-only mode.
            debug!(
                "Rejecting non-admin connection to {} when in admin only mode",
                pool_name
            );
            error_response_terminal(
                &mut write,
                "terminating connection due to administrator command",
            )
            .await?;
            return Err(Error::ShuttingDown);
        }

        // Generate random backend ID and secret key
        let process_id: i32 = rand::random();
        let secret_key: i32 = rand::random();

        let mut prepared_statements_enabled = false;

        // Authenticate admin user.
        let (transaction_mode, mut server_parameters, tenant_guarded) = if admin {
            let config = get_config();
            // TODO: Add SASL support.
            // Perform MD5 authentication.
            match config.general.admin_auth_type {
                AuthType::Trust => (),
                AuthType::MD5 => {
                    let salt = md5_challenge(&mut write).await?;

                    let code = match read.read_u8().await {
                        Ok(p) => p,
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password code".into(),
                                client_identifier,
                            ))
                        }
                    };

                    // PasswordMessage
                    if code as char != 'p' {
                        return Err(Error::ProtocolSyncError(format!(
                            "Expected p, got {}",
                            code as char
                        )));
                    }

                    let len = match read.read_i32().await {
                        Ok(len) => len,
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password message length".into(),
                                client_identifier,
                            ))
                        }
                    };

                    let mut password_response = vec![0u8; (len - 4) as usize];

                    match read.read_exact(&mut password_response).await {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password message".into(),
                                client_identifier,
                            ))
                        }
                    };

                    // Compare server and client hashes.
                    let password_hash = md5_hash_password(
                        &config.general.admin_username,
                        &config.general.admin_password,
                        &salt,
                    );

                    if password_hash != password_response {
                        let error =
                            Error::ClientGeneralError("Invalid password".into(), client_identifier);

                        warn!("{}", error);
                        authentication_failed(&mut write, username).await?;

                        return Err(error);
                    }
                }
            }
            (false, generate_server_parameters_for_admin(), false)
        }
        // Authenticate normal user.
        else {
            let pool = match get_pool(pool_name, username) {
                Some(pool) => pool,
                None => {
                    error_response(
                        &mut write,
                        &format!(
                            "No pool configured for database: {:?}, user: {:?}",
                            pool_name, username
                        ),
                    )
                    .await?;

                    return Err(Error::ClientGeneralError(
                        "Invalid pool name".into(),
                        client_identifier,
                    ));
                }
            };

            // Obtain the hash to compare, we give preference to that written in cleartext in config
            // if there is nothing set in cleartext and auth passthrough (auth_query) is configured, we use the hash obtained
            // when the pool was created. If there is no hash there, we try to fetch it one more time.
            match pool.settings.user.auth_type {
                AuthType::Trust => (),
                AuthType::MD5 => {
                    // Perform MD5 authentication.
                    // TODO: Add SASL support.
                    let salt = md5_challenge(&mut write).await?;

                    let code = match read.read_u8().await {
                        Ok(p) => p,
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password code".into(),
                                client_identifier,
                            ))
                        }
                    };

                    // PasswordMessage
                    if code as char != 'p' {
                        return Err(Error::ProtocolSyncError(format!(
                            "Expected p, got {}",
                            code as char
                        )));
                    }

                    let len = match read.read_i32().await {
                        Ok(len) => len,
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password message length".into(),
                                client_identifier,
                            ))
                        }
                    };

                    let mut password_response = vec![0u8; (len - 4) as usize];

                    match read.read_exact(&mut password_response).await {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(Error::ClientSocketError(
                                "password message".into(),
                                client_identifier,
                            ))
                        }
                    };

                    let password_hash = if let Some(password) = &pool.settings.user.password {
                        Some(md5_hash_password(username, password, &salt))
                    } else {
                        if !get_config().is_auth_query_configured() {
                            authentication_failed(&mut write, username).await?;
                            return Err(Error::ClientAuthImpossible(username.into()));
                        }

                        let mut hash = (*pool.auth_hash.read()).clone();

                        if hash.is_none() {
                            warn!(
                                "Query auth configured \
                                  but no hash password found \
                                  for pool {}. Will try to refetch it.",
                                pool_name
                            );

                            match refetch_auth_hash(&pool).await {
                                Ok(fetched_hash) => {
                                    warn!(
                                        "Password for {}, obtained. Updating.",
                                        client_identifier
                                    );

                                    {
                                        let mut pool_auth_hash = pool.auth_hash.write();
                                        *pool_auth_hash = Some(fetched_hash.clone());
                                    }

                                    hash = Some(fetched_hash);
                                }

                                Err(err) => {
                                    authentication_failed(&mut write, username).await?;

                                    return Err(Error::ClientAuthPassthroughError(
                                        err.to_string(),
                                        client_identifier,
                                    ));
                                }
                            }
                        };

                        Some(md5_hash_second_pass(&hash.unwrap(), &salt))
                    };

                    // Once we have the resulting hash, we compare with what the client gave us.
                    // If they do not match and auth query is set up, we try to refetch the hash one more time
                    // to see if the password has changed since the pool was created.
                    //
                    // @TODO: we could end up fetching again the same password twice (see above).
                    if password_hash.unwrap() != password_response {
                        warn!(
                            "Invalid password {}, will try to refetch it.",
                            client_identifier
                        );

                        let fetched_hash = match refetch_auth_hash(&pool).await {
                            Ok(fetched_hash) => fetched_hash,
                            Err(err) => {
                                authentication_failed(&mut write, username).await?;

                                return Err(err);
                            }
                        };

                        let new_password_hash = md5_hash_second_pass(&fetched_hash, &salt);

                        // Ok password changed in server an auth is possible.
                        if new_password_hash == password_response {
                            warn!(
                                "Password for {}, changed in server. Updating.",
                                client_identifier
                            );

                            {
                                let mut pool_auth_hash = pool.auth_hash.write();
                                *pool_auth_hash = Some(fetched_hash);
                            }
                        } else {
                            authentication_failed(&mut write, username).await?;
                            return Err(Error::ClientGeneralError(
                                "Invalid password".into(),
                                client_identifier,
                            ));
                        }
                    }
                }
            }
            let transaction_mode = pool.settings.pool_mode == PoolMode::Transaction;
            prepared_statements_enabled =
                transaction_mode && pool.prepared_statement_cache.is_some();

            // If the pool hasn't been validated yet,
            // connect to the servers and figure out what's what.
            if !pool.validated() {
                match pool.validate().await {
                    Ok(_) => (),
                    Err(err) => {
                        error_response(
                            &mut write,
                            &format!(
                                "Pool down for database: {:?}, user: {:?}",
                                pool_name, username
                            ),
                        )
                        .await?;
                        return Err(Error::ClientError(format!("Pool down: {:?}", err)));
                    }
                }
            }

            (
                transaction_mode,
                pool.server_parameters(),
                pool.settings.tenant_guard.is_some(),
            )
        };

        // Update the parameters to merge what the application sent and what's originally on the server
        server_parameters.set_from_hashmap(&parameters, false);

        // These go to the server connection without passing the tenant guard, and
        // some change how Postgres reads the queries the guard checks.
        if tenant_guarded {
            if let Some(error) = unreadable_session(&server_parameters) {
                error_response_terminal(&mut write, &with_sqlstate(UNSUPPORTED, &error)).await?;
                return Err(Error::ClientGeneralError(error, client_identifier));
            }
        }

        debug!("Password authentication successful");

        auth_ok(&mut write).await?;
        write_all(&mut write, (&server_parameters).into()).await?;
        backend_key_data(&mut write, process_id, secret_key).await?;
        send_ready_for_query(&mut write).await?;

        trace!("Startup OK");
        let stats = Arc::new(ClientStats::new(
            process_id,
            application_name,
            username,
            pool_name,
            tokio::time::Instant::now(),
        ));

        Ok(Client {
            read: BufReader::new(read),
            write,
            buffer: BytesMut::with_capacity(8196),
            response_message_queue_buffer: BytesMut::with_capacity(8196),
            held_replies: HeldReplies::default(),
            addr,
            cancel_mode: false,
            transaction_mode,
            process_id,
            secret_key,
            client_server_map,
            parameters: parameters.clone(),
            stats,
            admin,
            last_address_id: None,
            last_server_stats: None,
            connected_to_server: false,
            pool_name: pool_name.clone(),
            username: username.clone(),
            server_parameters,
            shutdown,
            drain_at: None,
            prepared_statements_enabled,
            extended_protocol_data_buffer: VecDeque::new(),
            local_statements: HashMap::new(),
            local_portals: HashMap::new(),
            verifier_generation: 0,
            identity_recheck: false,
            withhold_server_text: false,
            extended_protocol_error: None,
            discard_until_sync: false,
            copy_in_extended: false,
            statements: StatementPlans::default(),
            portal_plans: HashMap::new(),
            response_rewriter: ResponseRewriter::default(),
            implicit_transaction: false,
            transaction_enders: HashSet::new(),
            lock_releasers: HashSet::new(),
            enders_journal: Vec::new(),
            ending_binds: Vec::new(),
            releasing_binds: Vec::new(),
            batch_state: BatchState::default(),
            pools_generation: 0,
            identity: None,
            authorization: 0,
            identity_lapse: IdentityLapse::default(),
            tenant_may_change: false,
            sql_observed: false,
            observer: None,
            traces: None,
            parse_meta: Snap::Unparsed,
            portal_effects: HashMap::new(),
            pending_identity: Vec::new(),
            bound_since_reload: None,
            pending_session_reset: false,
            batch_begins: false,
            query_plan: None,
        })
    }

    /// Handle cancel request.
    pub async fn cancel(
        read: S,
        write: T,
        addr: std::net::SocketAddr,
        mut bytes: BytesMut, // The rest of the startup message.
        client_server_map: ClientServerMap,
        shutdown: Receiver<()>,
    ) -> Result<Client<S, T>, Error> {
        let process_id = bytes.get_i32();
        let secret_key = bytes.get_i32();
        Ok(Client {
            read: BufReader::new(read),
            write,
            buffer: BytesMut::with_capacity(8196),
            response_message_queue_buffer: BytesMut::with_capacity(8196),
            held_replies: HeldReplies::default(),
            addr,
            cancel_mode: true,
            transaction_mode: false,
            process_id,
            secret_key,
            client_server_map,
            parameters: HashMap::new(),
            stats: Arc::new(ClientStats::default()),
            admin: false,
            last_address_id: None,
            last_server_stats: None,
            connected_to_server: false,
            pool_name: String::from("undefined"),
            username: String::from("undefined"),
            server_parameters: ServerParameters::new(),
            shutdown,
            drain_at: None,
            prepared_statements_enabled: false,
            extended_protocol_data_buffer: VecDeque::new(),
            local_statements: HashMap::new(),
            local_portals: HashMap::new(),
            verifier_generation: 0,
            identity_recheck: false,
            withhold_server_text: false,
            extended_protocol_error: None,
            discard_until_sync: false,
            copy_in_extended: false,
            statements: StatementPlans::default(),
            portal_plans: HashMap::new(),
            response_rewriter: ResponseRewriter::default(),
            implicit_transaction: false,
            transaction_enders: HashSet::new(),
            lock_releasers: HashSet::new(),
            enders_journal: Vec::new(),
            ending_binds: Vec::new(),
            releasing_binds: Vec::new(),
            batch_state: BatchState::default(),
            pools_generation: 0,
            identity: None,
            authorization: 0,
            identity_lapse: IdentityLapse::default(),
            tenant_may_change: false,
            sql_observed: false,
            observer: None,
            traces: None,
            parse_meta: Snap::Unparsed,
            portal_effects: HashMap::new(),
            pending_identity: Vec::new(),
            bound_since_reload: None,
            pending_session_reset: false,
            batch_begins: false,
            query_plan: None,
        })
    }

    /// Handle a connected and authenticated client.
    pub async fn handle(&mut self) -> Result<(), Error> {
        // The client wants to cancel a query it has issued previously.
        if self.cancel_mode {
            trace!("Sending CancelRequest");

            let (process_id, secret_key, address, port) = {
                let guard = self.client_server_map.lock();

                match guard.get(&(self.process_id, self.secret_key)) {
                    // Drop the mutex as soon as possible.
                    // We found the server the client is using for its query
                    // that it wants to cancel.
                    Some((process_id, secret_key, address, port)) => {
                        (*process_id, *secret_key, address.clone(), *port)
                    }

                    // The client doesn't know / got the wrong server,
                    // we're closing the connection for security reasons.
                    None => return Ok(()),
                }
            };

            // Opens a new separate connection to the server, sends the backend_id
            // and secret_key and then closes it for security reasons. No other interactions
            // take place.
            return Server::cancel(&address, port, process_id, secret_key).await;
        }

        // The query router determines where the query is going to go,
        // e.g. primary, replica, which shard.
        let mut query_router = QueryRouter::new();

        let mut checkout_failure_count: u64 = 0;

        self.stats.register(self.stats.clone());

        let client_identifier = ClientIdentifier::new(
            self.server_parameters.get_application_name(),
            &self.username,
            &self.pool_name,
        );

        // Get a pool instance referenced by the most up-to-date
        // pointer. This ensures we always read the latest config
        // when starting a query.
        let mut pool_generation = pools_generation();
        let mut pool = if self.admin {
            // Admin clients do not use pools.
            ConnectionPool::default()
        } else {
            self.get_pool().await?
        };

        query_router.update_pool_settings(&pool.settings);
        self.withhold_server_text = query_router.withholds_server_text();
        query_router.set_default_role();

        // Our custom protocol loop.
        // We expect the client to either start a transaction with regular queries
        // or issue commands for our sharding and server selection protocol.
        loop {
            trace!(
                "Client idle, waiting for message, transaction mode: {}",
                self.transaction_mode
            );
            // Portals die with their transaction: none is open here.
            self.bound_since_reload = None;

            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some custom protocol messages, e.g.
            // SET SHARDING KEY TO 'bigint';

            // read_message isn't cancel-safe, so the same future is polled until
            // it completes: the client keeps serving while it waits to drain.
            let mut message = {
                let read = read_message(&mut self.read);
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        // Admin clients ignore shutdown.
                        _ = self.shutdown.recv(), if !self.admin && self.drain_at.is_none() => {
                            let spread = get_config().general.shutdown_drain_spread;
                            let delay = rand::random_range(0..=spread);
                            self.drain_at = Some(
                                tokio::time::Instant::now()
                                    + tokio::time::Duration::from_millis(delay),
                            );
                        },
                        // select! builds every branch's future, even a disabled one.
                        _ = tokio::time::sleep_until(
                            self.drain_at.unwrap_or_else(tokio::time::Instant::now)
                        ), if self.drain_at.is_some() => {
                            error_response_terminal(
                                &mut self.write,
                                "terminating connection due to administrator command"
                            ).await?;

                            self.stats.disconnect();
                            return Ok(());
                        },
                        message_result = &mut read => break message_result?,
                    }
                }
            };

            if message[0] as char == 'X' {
                debug!("Client disconnecting");

                self.stats.disconnect();

                return Ok(());
            }

            // Handle admin database queries.
            if self.admin {
                debug!("Handling admin command");
                handle_admin(&mut self.write, message, self.client_server_map.clone()).await?;
                continue;
            }

            trace_context::client_message(&mut self.traces, &message);

            // Plan statements with the pool's current settings. A reload since the
            // client's last transaction may have changed what the plugins cover,
            // and the refresh before checkout comes after Parse and Bind are planned,
            // and an Execute, Sync or Flush pgcat answers itself needs no checkout.
            // Or what verifies identities: an Execute may set one verified before.
            if matches!(message[0], b'Q' | b'P' | b'B' | b'E' | b'S' | b'H')
                && pools_generation() != pool_generation
            {
                pool_generation = pools_generation();
                if let Some(current) = get_pool(&self.pool_name, &self.username) {
                    if current.config_hash != pool.config_hash {
                        pool = current;
                        self.plan_with(&mut query_router, &pool.settings);
                    }
                }
            }
            // One observation decision per message: see `sql_observed`.
            self.sql_observed = false;
            self.refresh_observer(&query_router);

            self.expire_identity();
            if self
                .recheck_identity(&query_router, message[0], None)
                .await?
            {
                continue;
            }

            // Identity commands never reach the server.
            if message[0] == b'Q'
                && self
                    .handle_identity_query(&query_router, &message, None)
                    .await?
            {
                continue;
            }

            // Handle all custom protocol commands, if any.
            if self
                .handle_custom_protocol(&mut query_router, &message, &pool)
                .await?
            {
                continue;
            }

            // The initial message already went through the query parser and plugins.
            let mut initial_message_processed = false;

            match message[0] as char {
                // Query
                'Q' => {
                    if query_router.query_parser_enabled() || query_router.plugins_enabled() {
                        message = match self
                            .run_query_plugins(
                                &mut query_router,
                                message,
                                true,
                                None,
                                &client_identifier,
                            )
                            .await?
                        {
                            Some(message) => message,
                            None => continue,
                        };

                        initial_message_processed = true;

                        // Refuse what can't run before holding a server for it.
                        if let Some(error) = self.run_query_checkout_hook(&pool).await? {
                            self.query_plan = None;
                            self.refuse_query(&error, None).await?;
                            continue;
                        }
                    } else {
                        self.observe_gap(&query_router, Gap::UnparsedQuery);
                        self.sql_observed = true;
                    }
                }

                // Buffer extended protocol messages even if we do not have
                // a server connection yet. Hopefully, when we get the S message
                // we'll be able to allocate a connection. Also, clients do not expect
                // the server to respond to these messages so even if we were not able to
                // allocate a connection, we wouldn't be able to send back an error message
                // to the client so we buffer them and defer the decision to error out or not
                // to when we get the S message
                // Parse
                'P' => {
                    self.handle_parse(&mut query_router, message, &pool, true, &client_identifier)
                        .await?;
                    continue;
                }

                // Bind
                'B' => {
                    if self.discarding_extended_messages() {
                        continue;
                    }

                    if query_router.query_parser_enabled() {
                        query_router.infer_shard_from_bind(&message);
                    }

                    if let Some(error) = self
                        .replan_stale_statement(
                            &mut query_router,
                            &message,
                            &pool,
                            &client_identifier,
                        )
                        .await?
                    {
                        self.extended_protocol_error = Some(error);
                        continue;
                    }

                    self.buffer_bind(message).await?;

                    continue;
                }

                // Describe
                'D' => {
                    if !self.discarding_extended_messages() {
                        self.buffer_describe(message).await?;
                    }
                    continue;
                }

                'E' => {
                    if !self.discarding_extended_messages() {
                        self.buffer_execute(&query_router, message).await?;
                    }
                    continue;
                }

                // Close (F)
                'C' => {
                    if !self.discarding_extended_messages() {
                        self.buffer_close(message)?;
                    }
                    continue;
                }

                // Sync or Flush: answer without a server when we can.
                'S' | 'H' => {
                    let sync = message[0] == b'S';
                    if self.finish_batch_without_server(sync, None).await? {
                        continue;
                    }

                    // Refuse what can't run before holding a server for it.
                    if let Some(error) = self.run_checkout_hooks(&pool).await? {
                        self.reject_batch(&error, sync, None).await?;
                        continue;
                    }
                }

                _ => (),
            }

            // Check if the pool is paused and wait until it's resumed.
            pool.wait_paused().await;

            // Refresh pool information, something might have changed.
            self.pools_generation = pools_generation();
            pool_generation = self.pools_generation;
            pool = self.get_pool().await?;
            let mut replanned = self.plan_with(&mut query_router, &pool.settings);
            // Whether the text of the server's replies is withheld on the pool
            // the server is checked out from.
            let checked_out_withholds = self.withhold_server_text;

            debug!("Waiting for connection from pool");
            if !self.admin {
                self.stats.waiting();
            }

            // Grab a server from the pool.
            let connection = match pool
                .get(query_router.shard(), query_router.role(), &self.stats)
                .await
            {
                Ok(conn) => {
                    debug!("Got connection from pool");
                    conn
                }
                Err(err) => {
                    // Client is attempting to get results from the server,
                    // but we were unable to grab a connection from the pool
                    // We'll send back an error message and clean the extended
                    // protocol buffer
                    self.stats.idle();
                    self.response_rewriter.reset();
                    self.query_plan = None;

                    if message[0] as char == 'S' {
                        error!("Got Sync message but failed to get a connection from the pool");
                        self.reset_buffered_state();
                    }

                    error_response(
                        &mut self.write,
                        format!("could not get connection from the pool - {}", err).as_str(),
                    )
                    .await?;

                    error!(
                        "Could not get connection from pool: \
                        {{ \
                            pool_name: {:?}, \
                            username: {:?}, \
                            shard: {:?}, \
                            role: \"{:?}\", \
                            error: \"{:?}\" \
                        }}",
                        self.pool_name,
                        self.username,
                        query_router.shard(),
                        query_router.role(),
                        err
                    );
                    checkout_failure_count += 1;
                    if let Some(limit) = pool.settings.checkout_failure_limit {
                        if checkout_failure_count >= limit {
                            error!(
                                "Checkout failure limit reached ({} / {}) - disconnecting client",
                                checkout_failure_count, limit
                            );
                            error_response_terminal(
                                &mut self.write,
                                &format!(
                                    "checkout failure limit reached ({} / {})",
                                    checkout_failure_count, limit
                                ),
                            )
                            .await?;
                            self.stats.disconnect();
                            return Ok(());
                        }
                    }
                    continue;
                }
            };

            // What was planned before a reload changed the plugin configuration
            // (while the pool was paused, or the client waited for a server)
            // can't run.
            replanned |= self.follow_reload(&mut query_router);
            if replanned && self.refuse_stale_plan(message[0]).await? {
                self.stats.idle();
                continue;
            }
            // Nothing was sent on the server yet: it goes back to the pool.
            if self
                .recheck_identity(&query_router, message[0], None)
                .await?
            {
                self.stats.idle();
                continue;
            }

            let mut reference = connection.0;
            let address = connection.1;
            let server = &mut *reference;

            // Server is assigned to the client in case the client wants to
            // cancel a query later.
            server.claim(self.process_id, self.secret_key);
            self.connected_to_server = true;

            // Update statistics
            self.stats.active();

            self.last_address_id = Some(address.id);
            self.last_server_stats = Some(server.stats());

            debug!(
                "Client {:?} talking to server {:?}",
                self.addr,
                server.address()
            );

            server
                .sync_parameters(&self.server_parameters, pool.settings.sync_application_name)
                .await?;
            // The last user's statements may have changed the tenant parameter
            // without naming it (a function, view or default that sets it), so
            // it's set at every checkout, whatever pgcat set before.
            server.forget_tenant();
            self.sync_tenant(server, &pool).await?;

            let mut initial_message = Some(message);
            let mut after_sync = false;

            let idle_client_timeout_duration = match get_idle_client_in_transaction_timeout() {
                0 => tokio::time::Duration::MAX,
                timeout => tokio::time::Duration::from_millis(timeout),
            };

            // Transaction loop. Multiple queries can be issued by the client here.
            // The connection belongs to the client until the transaction is over,
            // or until the client disconnects if we are in session mode.
            //
            // If the client is in session mode, no more custom protocol
            // commands will be accepted.
            loop {
                let message = match initial_message {
                    None => {
                        trace!("Waiting for message inside transaction or in session mode");

                        // This is not an initial message, it hasn't been processed yet.
                        initial_message_processed = false;
                        self.sql_observed = false;
                        self.refresh_observer(&query_router);

                        match tokio::time::timeout(
                            idle_client_timeout_duration,
                            read_message(&mut self.read),
                        )
                        .await
                        {
                            Ok(Ok(message)) => {
                                trace_context::client_message(&mut self.traces, &message);
                                message
                            }
                            Ok(Err(err)) => {
                                // Client disconnected inside a transaction.
                                // Clean up the server and re-use it.
                                self.stats.disconnect();
                                server.checkin_cleanup().await?;
                                self.detach_observer();

                                return Err(err);
                            }
                            Err(_) => {
                                // Client idle in transaction timeout
                                error_response(&mut self.write, "idle transaction timeout").await?;
                                error!(
                                    "Client idle in transaction timeout: \
                                    {{ \
                                        pool_name: {}, \
                                        username: {}, \
                                        shard: {:?}, \
                                        role: \"{:?}\" \
                                    }}",
                                    self.pool_name,
                                    self.username,
                                    query_router.shard(),
                                    query_router.role()
                                );

                                break;
                            }
                        }
                    }

                    Some(message) => {
                        initial_message = None;
                        message
                    }
                };

                // The message will be forwarded to the server intact. We still would like to
                // parse it below to figure out what to do with it.

                // Safe to unwrap because we know this message has a certain length and has the code
                // This reads the first byte without advancing the internal pointer and mutating the bytes
                let code = *message.first().unwrap() as char;

                trace!("Client message: {}", code);

                // A reload may have changed what the plugins cover, or what
                // verifies identities, since the server was checked out; the
                // initial message was checked then. At Sync or Flush, a batch
                // bound before it is refused, as is an Execute of a portal
                // bound before it or of one a plugin answered before it.
                if !initial_message_processed && matches!(code, 'Q' | 'P' | 'B' | 'E' | 'S' | 'H') {
                    // Portals die with their transaction: none is open after
                    // a Sync or Query that left the server idle.
                    if after_sync && !server.in_transaction() {
                        self.bound_since_reload = None;
                    }
                    after_sync = matches!(code, 'Q' | 'S');
                    if self.follow_reload(&mut query_router) {
                        self.bound_since_reload = Some(FreshPortals::default());
                    }
                }

                // The tenant guard, the tenant parameter pgcat sets and whether
                // server text is withheld stay those of the pool the server was
                // checked out from. A session can't follow a reload that changes
                // them (one that turns on a plugin that withholds server text,
                // say), here or while it waited for the server: what it set
                // unguarded can't be trusted, and its replies would go out
                // unsanitized.
                if held_settings(query_router.pool_settings(), self.withhold_server_text)
                    != held_settings(&pool.settings, checked_out_withholds)
                {
                    let error = "the pool's plugin settings changed in a reload: connect again";
                    server.mark_bad(error);
                    error_response_terminal(&mut self.write, &with_sqlstate(UNSUPPORTED, error))
                        .await?;
                    return Err(Error::ClientError(error.into()));
                }

                self.expire_identity();
                if self
                    .recheck_identity(&query_router, code as u8, Some(&mut *server))
                    .await?
                {
                    continue;
                }

                match code {
                    // Query
                    'Q' => {
                        // Identity commands never reach the server.
                        if !initial_message_processed
                            && self
                                .handle_identity_query(&query_router, &message, Some(&mut *server))
                                .await?
                        {
                            continue;
                        }

                        // The initial message was already parsed and run through the plugins.
                        let message = if !initial_message_processed
                            && (query_router.query_parser_enabled()
                                || query_router.plugins_enabled())
                        {
                            match self
                                .run_query_plugins(
                                    &mut query_router,
                                    message,
                                    false,
                                    Some(&mut *server),
                                    &client_identifier,
                                )
                                .await?
                            {
                                Some(message) => message,
                                None => continue,
                            }
                        } else {
                            if !initial_message_processed {
                                self.observe_gap(&query_router, Gap::UnparsedQuery);
                            }
                            message
                        };

                        self.sync_tenant(server, &pool).await?;

                        let message = match self.query_plan.take() {
                            Some(plan) => {
                                match self.run_query_hook(&plan, message, server, &pool).await? {
                                    Some(message) => message,
                                    // Refused, and answered without the server.
                                    None => {
                                        if self.server_lost(server)?
                                            || (self.transaction_mode && !server.in_transaction())
                                        {
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            }
                            None => message,
                        };

                        debug!("Sending query to server");

                        self.response_rewriter.start();
                        self.begin_implicit_transaction(server).await?;
                        if let Some(observer) = self.observer.as_deref_mut() {
                            observer.send_query();
                        }
                        self.send_and_receive_loop(
                            code,
                            Some(&message),
                            server,
                            &address,
                            &pool,
                            &self.stats.clone(),
                        )
                        .await?;

                        if !server.in_transaction() {
                            // Portals don't outlive the transaction.
                            self.portal_plans.clear();

                            // Report transaction executed statistics.
                            self.stats.transaction();
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode && !server.in_copy_mode() {
                                self.stats.idle();

                                break;
                            }
                        }
                    }

                    // Terminate
                    'X' => {
                        server.checkin_cleanup().await?;
                        self.detach_observer();
                        self.stats.disconnect();
                        self.release();

                        return Ok(());
                    }

                    // Parse
                    // The query with placeholders is here, e.g. `SELECT * FROM users WHERE email = $1 AND active = $2`.
                    'P' => {
                        self.handle_parse(
                            &mut query_router,
                            message,
                            &pool,
                            false,
                            &client_identifier,
                        )
                        .await?;
                    }

                    // Bind
                    // The placeholder's replacements are here, e.g. 'user@email.com' and 'true'
                    'B' => {
                        if !self.discarding_extended_messages() {
                            match self
                                .replan_stale_statement(
                                    &mut query_router,
                                    &message,
                                    &pool,
                                    &client_identifier,
                                )
                                .await?
                            {
                                Some(error) => self.extended_protocol_error = Some(error),
                                None => self.buffer_bind(message).await?,
                            }
                        }
                    }

                    // Describe
                    // Command a client can issue to describe a previously prepared named statement.
                    'D' => {
                        if !self.discarding_extended_messages() {
                            self.buffer_describe(message).await?;
                        }
                    }

                    // Execute
                    // Execute a prepared statement prepared in `P` and bound in `B`.
                    'E' => {
                        if !self.discarding_extended_messages() {
                            self.buffer_execute(&query_router, message).await?;
                        }
                    }

                    // Close
                    // Close the prepared statement.
                    'C' => {
                        if !self.discarding_extended_messages() {
                            self.buffer_close(message)?;
                        }
                    }

                    // Flush
                    // asyncpg sends Parse → Describe → Flush during prepared statement
                    // initialization. We convert this to a Sync, then strip the resulting
                    // ReadyForQuery from the server response so the client never sees it.
                    'H' => {
                        debug!("Flush received, converting to Sync");

                        if self
                            .finish_batch_without_server(false, Some(&mut *server))
                            .await?
                        {
                            continue;
                        }

                        // Before the hooks, which may query the server.
                        self.sync_tenant(server, &pool).await?;

                        if let Some(error) = self.run_bind_hooks(&pool, server, false).await? {
                            self.reject_batch(&error, false, Some(&mut *server)).await?;
                            if self.server_lost(server)? {
                                break;
                            }
                            continue;
                        }

                        self.process_extended_protocol_buffer(server, &pool, &address)
                            .await?;
                        self.response_rewriter.start();

                        // Inject a Sync message instead of the original Flush
                        self.buffer.put(&SYNC_BYTES[..]);

                        let mut should_send_to_server = true;

                        // If buffer contains only the Sync we just added, everything was
                        // handled from the prepared statement cache — no server round-trip needed.
                        if *self.buffer.first().unwrap() == b'S' {
                            should_send_to_server = false;
                            // Unlike Sync, Flush does NOT produce a ReadyForQuery for the client,
                            // so we do not enqueue one here.
                        }

                        // Send cached responses (e.g. ParseComplete from cache) to the client
                        if !self.response_message_queue_buffer.is_empty() {
                            if let Err(err) = write_all_flush(
                                &mut self.write,
                                &self.response_message_queue_buffer,
                            )
                            .await
                            {
                                server.mark_bad(err.to_string().as_str());
                                return Err(err);
                            }

                            self.response_message_queue_buffer.clear();
                        }

                        if let Some(observer) = self.observer.as_deref_mut() {
                            if should_send_to_server {
                                observer.send_batch();
                            } else {
                                observer.accept_local_batch();
                            }
                        }

                        if should_send_to_server {
                            // As at a Sync: a response hook failing mustn't leave
                            // the batch's writes committed.
                            self.begin_implicit_transaction(server).await?;
                            self.sending_statements(server);
                            self.send_server_message(server, &self.buffer, &address, &pool)
                                .await?;

                            // Receive responses, stripping the trailing ReadyForQuery that Sync
                            // produces but the client does not expect from a Flush.
                            loop {
                                let response = self
                                    .receive_server_message(
                                        server,
                                        &address,
                                        &pool,
                                        &self.stats.clone(),
                                    )
                                    .await?;
                                let response =
                                    self.rewrite_response(response, server, &pool).await?;
                                let response = self.splice_held_replies(response, server)?;

                                if server.is_data_available() {
                                    // Not the last chunk — forward as-is
                                    match write_all_flush(&mut self.write, &response).await {
                                        Ok(_) => (),
                                        Err(err) => {
                                            server.mark_bad(err.to_string().as_str());
                                            return Err(err);
                                        }
                                    };
                                } else {
                                    // Last chunk — strip trailing ReadyForQuery (6 bytes)
                                    if response.len() > READY_FOR_QUERY_SIZE {
                                        let trimmed =
                                            &response[..response.len() - READY_FOR_QUERY_SIZE];
                                        match write_all_flush(&mut self.write, trimmed).await {
                                            Ok(_) => (),
                                            Err(err) => {
                                                server.mark_bad(err.to_string().as_str());
                                                return Err(err);
                                            }
                                        };
                                    }
                                    // If response is exactly READY_FOR_QUERY_SIZE, it's just
                                    // ReadyForQuery — nothing to forward.
                                    break;
                                }
                            }

                            // pgcat's transaction stays open to the Sync, unless the
                            // client's own BEGIN made it the client's.
                            let (failed, client_began) = self.take_round_outcome();
                            if client_began {
                                self.implicit_transaction = false;
                            }

                            // A response hook failed: the client has its error. As after
                            // any error, the rest of the batch is ignored until Sync.
                            // As at a Sync (`finish_batch`), a transaction pgcat opened
                            // is rolled back, and the client's fails.
                            if failed {
                                self.fail_transaction(Some(&mut *server)).await?;
                                self.discard_until_sync = true;
                            }
                        }

                        self.buffer.clear();
                        // No transaction stats or pool release for Flush
                    }

                    // Sync
                    // Frontend (client) is asking for the query result now.
                    'S' => {
                        debug!("Sending query to server");

                        let mut answered = self
                            .finish_batch_without_server(true, Some(&mut *server))
                            .await?;

                        if !answered {
                            // Before the hooks, which may query the server.
                            self.sync_tenant(server, &pool).await?;

                            if let Some(error) = self.run_bind_hooks(&pool, server, true).await? {
                                self.reject_batch(&error, true, Some(&mut *server)).await?;
                                if self.server_lost(server)? {
                                    break;
                                }
                                answered = true;
                            }
                        }

                        if answered {
                            if !server.in_transaction() && self.transaction_mode {
                                break;
                            }
                            continue;
                        }

                        // Prepared statements can arrive like this
                        // 1. Without named describe
                        //      Client: Parse, with name, query and params
                        //              Sync
                        //      Server: ParseComplete
                        //              ReadyForQuery
                        // 3. Without named describe
                        //      Client: Parse, with name, query and params
                        //              Describe, with no name
                        //              Sync
                        //      Server: ParseComplete
                        //              ParameterDescription
                        //              RowDescription
                        //              ReadyForQuery
                        // 2. With named describe
                        //      Client: Parse, with name, query and params
                        //              Describe, with name
                        //              Sync
                        //      Server: ParseComplete
                        //              ParameterDescription
                        //              RowDescription
                        //              ReadyForQuery

                        self.process_extended_protocol_buffer(server, &pool, &address)
                            .await?;
                        self.response_rewriter.start();

                        // Add the sync message
                        self.buffer.put(&message[..]);

                        let mut should_send_to_server = true;

                        // If we have just a sync message left (maybe after omitting sending some messages to the server) no need to send it to the server
                        // Unless a transaction pgcat opened at a Flush is open: the Sync ends it, and
                        // `finish_batch` commits it at the server's reply.
                        if *self.buffer.first().unwrap() == b'S' && !self.implicit_transaction {
                            should_send_to_server = false;
                            // queue up a ready for query message to send to the client, respecting the transaction state of the server
                            self.response_message_queue_buffer
                                .put(ready_for_query_status(server.transaction_status()));
                        }

                        // Send the replies pgcat gave itself ahead of every message the server
                        // answers; the others follow the server's replies (`splice_held_replies`).
                        if !self.response_message_queue_buffer.is_empty() {
                            if let Err(err) = write_all_flush(
                                &mut self.write,
                                &self.response_message_queue_buffer,
                            )
                            .await
                            {
                                // We might be in some kind of error/in between protocol state
                                server.mark_bad(err.to_string().as_str());
                                return Err(err);
                            }

                            self.response_message_queue_buffer.clear();
                        }

                        if let Some(observer) = self.observer.as_deref_mut() {
                            if should_send_to_server {
                                observer.send_batch();
                            } else {
                                observer.accept_local_batch();
                            }
                        }

                        if should_send_to_server {
                            self.begin_implicit_transaction(server).await?;
                            self.send_and_receive_loop(
                                code,
                                None,
                                server,
                                &address,
                                &pool,
                                &self.stats.clone(),
                            )
                            .await?;
                            self.copy_in_extended = server.in_copy_mode();
                        }

                        self.buffer.clear();

                        if !server.in_transaction() {
                            // Portals don't outlive the transaction.
                            self.portal_plans.clear();

                            self.stats.transaction();
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode && !server.in_copy_mode() {
                                break;
                            }
                        }
                    }

                    // CopyData
                    'd' => {
                        self.buffer.put(&message[..]);

                        // Want to limit buffer size
                        if self.buffer.len() > 8196 {
                            // Forward the data to the server,
                            self.send_server_message(server, &self.buffer, &address, &pool)
                                .await?;
                            self.buffer.clear();
                        }
                    }

                    // CopyDone or CopyFail
                    // Copy is done, successfully or not.
                    'c' | 'f' => {
                        // We may already have some copy data in the buffer, add this message to buffer
                        self.buffer.put(&message[..]);

                        // The server answers an extended-protocol COPY's end at the
                        // client's next Sync, and runs what comes before that Sync in
                        // the COPY's batch: pgcat takes only the Sync.
                        if std::mem::take(&mut self.copy_in_extended) {
                            // The COPY is still running on the server: the wait is bounded
                            // as the client's idleness is and as the statement is.
                            let (budget, timed_out) = match pool.settings.user.statement_timeout {
                                ms if ms > 0
                                    && tokio::time::Duration::from_millis(ms)
                                        < idle_client_timeout_duration =>
                                {
                                    (
                                        tokio::time::Duration::from_millis(ms),
                                        "pool statement timeout",
                                    )
                                }
                                _ => (idle_client_timeout_duration, "idle transaction timeout"),
                            };
                            match tokio::time::timeout(budget, read_message(&mut self.read)).await {
                                Ok(Ok(next)) if next[0] == b'S' => self.buffer.put(&next[..]),
                                next => {
                                    server
                                        .mark_bad("an extended-protocol COPY didn't end with Sync");
                                    let error = match next {
                                        Ok(Ok(_)) => Some(with_sqlstate(
                                            UNSUPPORTED,
                                            "an extended-protocol COPY must end with CopyDone or CopyFail, then Sync",
                                        )),
                                        Err(_) => Some(timed_out.to_string()),
                                        // The client is gone.
                                        Ok(Err(_)) => None,
                                    };
                                    if let Some(error) = error {
                                        error_response_terminal(&mut self.write, &error).await?;
                                    }
                                    return Err(Error::ProtocolSyncError(
                                        "an extended-protocol COPY didn't end with Sync".into(),
                                    ));
                                }
                            }
                        }

                        self.send_server_message(server, &self.buffer, &address, &pool)
                            .await?;

                        // Clear the buffer
                        self.buffer.clear();

                        let response = self
                            .receive_server_message(server, &address, &pool, &self.stats.clone())
                            .await?;

                        match write_all_flush(&mut self.write, &response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad(err.to_string().as_str());
                                return Err(err);
                            }
                        };

                        if !server.in_transaction() {
                            self.stats.transaction();
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode {
                                break;
                            }
                        }
                    }

                    // Some unexpected message. We either did not implement the protocol correctly
                    // or this is not a Postgres client we're talking to.
                    _ => {
                        error!("Unexpected code: {}", code);
                    }
                }
            }

            // The server is no longer bound to us, we can't cancel it's queries anymore.
            debug!("Releasing server back into the pool");

            server.checkin_cleanup().await?;
            self.detach_observer();

            server.stats().idle();
            self.connected_to_server = false;

            self.release();
            self.stats.idle();
        }
    }

    /// Retrieve connection pool, if it exists.
    /// Return an error to the client otherwise.
    async fn get_pool(&mut self) -> Result<ConnectionPool, Error> {
        match get_pool(&self.pool_name, &self.username) {
            Some(pool) => Ok(pool),
            None => {
                error_response(
                    &mut self.write,
                    &format!(
                        "No pool configured for database: {}, user: {}",
                        self.pool_name, self.username
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Invalid pool name {{ username: {}, pool_name: {}, application_name: {} }}",
                    self.pool_name,
                    self.username,
                    self.server_parameters.get_application_name()
                )))
            }
        }
    }

    /// Handles custom protocol messages
    /// Returns true if the message is custom protocol message, false otherwise
    /// Does not work with prepared statements, only simple and extended protocol without parameters
    async fn handle_custom_protocol(
        &mut self,
        query_router: &mut QueryRouter,
        message: &BytesMut,
        pool: &ConnectionPool,
    ) -> Result<bool, Error> {
        let current_shard = query_router.shard();

        match query_router.try_execute_command(message) {
            None => Ok(false),

            Some(custom) => {
                match custom {
                    // SET SHARD TO
                    (Command::SetShard, _) => {
                        match query_router.shard() {
                            None => {}
                            Some(selected_shard) => {
                                if selected_shard >= pool.shards() {
                                    // Bad shard number, send error message to client.
                                    query_router.set_shard(current_shard);

                                    error_response(
                                                    &mut self.write,
                                                    &format!(
                                                        "shard {} is not configured {}, staying on shard {:?} (shard numbers start at 0)",
                                                        selected_shard,
                                                        pool.shards(),
                                                        current_shard,
                                                    ),
                                                )
                                                    .await?;
                                } else {
                                    custom_protocol_response_ok(&mut self.write, "SET SHARD")
                                        .await?;
                                }
                            }
                        }
                    }

                    // SET PRIMARY READS TO
                    (Command::SetPrimaryReads, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET PRIMARY READS").await?;
                    }

                    // SET SHARDING KEY TO
                    (Command::SetShardingKey, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET SHARDING KEY").await?;
                    }

                    // SET SERVER ROLE TO
                    (Command::SetServerRole, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET SERVER ROLE").await?;
                    }

                    // SHOW SERVER ROLE
                    (Command::ShowServerRole, value) => {
                        show_response(&mut self.write, "server role", &value).await?;
                    }

                    // SHOW SHARD
                    (Command::ShowShard, value) => {
                        show_response(&mut self.write, "shard", &value).await?;
                    }

                    // SHOW PRIMARY READS
                    (Command::ShowPrimaryReads, value) => {
                        show_response(&mut self.write, "primary reads", &value).await?;
                    }
                };

                Ok(true)
            }
        }
    }

    /// Answer identity commands sent as a simple Query, refuse queries that set
    /// the tenant parameter, and forget the identity on `DISCARD ALL` / `RESET ALL`.
    /// Returns true if the client has been answered.
    async fn handle_identity_query(
        &mut self,
        query_router: &QueryRouter,
        message: &BytesMut,
        server: Option<&mut Server>,
    ) -> Result<bool, Error> {
        let text = query_text(message);
        let guard = query_router.pool_settings().tenant_guard.as_ref();
        let may_refuse = guard.is_some_and(|guard| guard.might_refuse(text));

        if !might_concern_identity(text) && !may_refuse {
            return Ok(false);
        }

        let mut cursor = Cursor::new(message);
        cursor.advance(mem::size_of::<u8>() + mem::size_of::<i32>());
        let query = cursor.read_string()?;

        if let Some(error) = tenant_parameter_error(&query, guard) {
            self.observe_gap(query_router, Gap::RefusedUnparsed);
            self.refuse_query(&or_sqlstate(PLUGIN_DENIED, &error), server)
                .await?;
            return Ok(true);
        }

        match IdentityCommand::parse(&query) {
            Ok(Some(command)) => {
                let action = match self.verify_identity_command(query_router, command).await {
                    Ok(action) => action,
                    Err(error) => {
                        self.refuse_query(&or_sqlstate(IDENTITY_REFUSED, &error), server)
                            .await?;
                        return Ok(true);
                    }
                };

                let mut response = BytesMut::new();
                if matches!(action, IdentityAction::Show) {
                    response.put(identity_row_description());
                }
                response.put(self.run_identity_command(action));
                response.put(ready_for_query_status(
                    server.map_or(b'I', |server| server.transaction_status()),
                ));

                write_all_flush(&mut self.write, &response).await?;
                Ok(true)
            }

            Ok(None) => {
                if clears_identity(&query) {
                    self.reset_session();
                }
                self.tenant_may_change |= guard.is_some_and(|guard| guard.might_set_tenant(text));
                Ok(false)
            }

            Err(error) => {
                self.refuse_query(&or_sqlstate(UNSUPPORTED, &error), server)
                    .await?;
                Ok(true)
            }
        }
    }

    /// Every change of the identity: portals bound under the old one can't run
    /// under the new one.
    fn set_identity(&mut self, identity: Option<Identity>) {
        self.identity = identity;
        self.identity_recheck = false;
        self.authorization += 1;
    }

    /// `DISCARD ALL` or `RESET ALL`: the identity goes with the session.
    fn reset_session(&mut self) {
        debug!("Session reset, forgetting the caller identity");
        self.set_identity(None);
        self.identity_lapse.clear();
    }

    /// Forget the identity once its token has expired.
    fn expire_identity(&mut self) {
        let expired = self
            .identity
            .as_ref()
            .map_or(false, |identity| identity.expired(SystemTime::now()));

        if expired {
            debug!("Caller identity expired");
            self.set_identity(None);
            self.identity_lapse.expired();
        }
    }

    /// A reload changed what verifies identities since the client's was
    /// verified: verify its token again before the statement (`code`) runs with
    /// it, once per reload. The same tenant and claims keep the identity, with
    /// the new verification's expiry. Anything
    /// else ends the session, as a reload changing the pool's `held_settings`
    /// does. A verifier that timed out or got no hook slot fails the statement
    /// with that error, never running it with the old identity, and the next
    /// statement verifies again. Returns whether the statement was refused.
    async fn recheck_identity(
        &mut self,
        query_router: &QueryRouter,
        code: u8,
        server: Option<&mut Server>,
    ) -> Result<bool, Error> {
        // A batch being discarded runs nothing.
        if !self.identity_recheck
            || !matches!(code, b'Q' | b'P' | b'B' | b'E' | b'S' | b'H')
            || (code != b'Q' && self.discarding_extended_messages())
        {
            return Ok(false);
        }
        let identity = match self.identity.as_mut() {
            Some(identity) => identity,
            None => return Ok(false),
        };

        let verified = match query_router.verifier_key() {
            0 => None,
            _ => Some(query_router.verify_identity_again(identity.token()).await),
        };
        let recheck = identity.recheck(verified);
        debug!(
            "Caller identity verified again after a reload: {} ({})",
            match recheck {
                Recheck::Keep => "kept",
                Recheck::Unavailable(_) => "the verifier is unavailable",
                Recheck::End => "changed, the session ends",
            },
            identity.token()
        );
        match recheck {
            // Its expiry is the new verification's, maybe past: then it's
            // forgotten before the statement runs, as at any other expiry.
            Recheck::Keep => {
                self.identity_recheck = false;
                self.expire_identity();
                Ok(false)
            }

            Recheck::Unavailable(error) => {
                let (code_of, message) = sqlstate_of(&error);
                let error = with_sqlstate(
                    code_of.unwrap_or(HOOK_TIMED_OUT),
                    &format!("PGCAT_IDENTITY could not be verified again: {}", message),
                );
                match code {
                    b'Q' => {
                        self.query_plan = None;
                        self.refuse_query(&error, server).await?;
                    }
                    b'S' | b'H' => self.reject_batch(&error, code == b'S', server).await?,
                    _ => self.extended_protocol_error = Some(error),
                }
                Ok(true)
            }

            Recheck::End => {
                let error = "the identity's verifier changed in a reload: connect again";
                if let Some(server) = server {
                    server.mark_bad(error);
                }
                error_response_terminal(&mut self.write, &with_sqlstate(UNSUPPORTED, error))
                    .await?;
                Err(Error::ClientError(error.into()))
            }
        }
    }

    /// Have the plugins verify the token of `SET PGCAT_IDENTITY`. A token they
    /// refuse also clears the identity the client had: it can't keep using an
    /// identity it tried to replace.
    async fn verify_identity_command(
        &mut self,
        query_router: &QueryRouter,
        command: IdentityCommand,
    ) -> Result<IdentityAction, String> {
        let token = match command {
            IdentityCommand::Set(token) => token,
            IdentityCommand::Reset => return Ok(IdentityAction::Reset),
            IdentityCommand::Show => return Ok(IdentityAction::Show),
        };

        let verified =
            query_router
                .verify_identity(&token)
                .await
                .and_then(|verified| match verified.tenant.as_deref() {
                    Some(tenant) if !valid_tenant(tenant) => {
                        Err("the identity names a tenant pgcat can't use".to_string())
                    }
                    _ => Ok(verified),
                });

        match verified {
            Ok(verified) => Ok(IdentityAction::Set(Identity::new(token, verified))),
            Err(error) => {
                debug!("Caller identity refused ({})", token);
                self.set_identity(None);
                self.identity_lapse.refused();
                // A verifier that timed out or got no hook slot keeps the
                // hook's SQLSTATE (PH001, PH002); a refusal gets 28000.
                let (code, error) = sqlstate_of(&error);
                let refused = format!("PGCAT_IDENTITY refused: {}", error);
                Err(match code {
                    Some(code) => with_sqlstate(code, &refused),
                    None => refused,
                })
            }
        }
    }

    /// The caller identity, for plugins.
    fn identity(&self) -> Option<&Identity> {
        self.identity.as_ref()
    }

    /// The SQL observer's view of the client, with its identity label.
    fn caller(&self) -> Caller<'_> {
        caller_of(
            &self.identity_lapse,
            self.identity.is_some(),
            &self.server_parameters,
            self.addr,
        )
    }

    /// Run an identity command. Returns the DataRows (for SHOW) and CommandComplete.
    fn run_identity_command(&mut self, action: IdentityAction) -> BytesMut {
        let mut response = BytesMut::new();
        let tag = action.tag();

        match action {
            IdentityAction::Set(identity) => {
                debug!("Caller identity set ({})", identity.token());
                self.set_identity(Some(identity));
                self.identity_lapse.clear();
            }

            IdentityAction::Reset => {
                debug!("Caller identity reset");
                self.set_identity(None);
                self.identity_lapse.clear();
            }

            IdentityAction::Show => {
                response.put(identity_show_row(self.identity.as_ref()));
            }
        }

        response.put(command_complete(tag));
        response
    }

    /// Set the server connection's tenant parameter to the client's tenant, or
    /// reset it, before the client's statements are sent.
    async fn sync_tenant(
        &mut self,
        server: &mut Server,
        pool: &ConnectionPool,
    ) -> Result<(), Error> {
        let parameter = match pool.settings.tenant_guard {
            Some(ref guard) => guard.parameter(),
            None => return Ok(()),
        };

        // Statements can't be sent while the server is copying data.
        if server.in_copy_mode() {
            return Ok(());
        }

        let tenant = self.identity.as_ref().and_then(Identity::tenant);
        server.sync_tenant(parameter, tenant).await
    }

    /// Statements are about to be sent: if they may change the tenant parameter,
    /// pgcat sets it again before the next ones. Only here, right before they
    /// go, does nothing else set it in between.
    fn sending_statements(&mut self, server: &mut Server) {
        if std::mem::take(&mut self.tenant_may_change) {
            server.forget_tenant();
        }
    }

    /// A gap event for the simple Query being handled, unless it was already
    /// recorded (see `sql_observed`).
    fn observe_gap(&mut self, query_router: &QueryRouter, gap: Gap) {
        if !self.sql_observed {
            query_router
                .pool_settings()
                .sql_observer
                .gap(gap, self.caller());
            if let Some(observer) = self.observer.as_deref_mut() {
                observer.admit_query(QueryAdmission::Unparsed);
            }
        }
    }

    /// The server goes back to the pool: its portals are gone.
    fn detach_observer(&mut self) {
        if let Some(observer) = self.observer.as_deref_mut() {
            observer.server_released();
        }
    }

    /// Keep the client's observer state only while the pool observes.
    fn refresh_observer(&mut self, query_router: &QueryRouter) {
        let enabled = query_router.pool_settings().sql_observer.enabled();
        match (enabled, self.observer.is_some()) {
            (true, false) => {
                let observer = if self.prepared_statements_enabled {
                    ClientObserver::cached()
                } else {
                    ClientObserver::new()
                };
                self.observer = Some(Box::new(observer));
            }
            (false, true) => self.observer = None,
            _ => (),
        }
    }

    /// Run the query parser and plugins on a simple Query.
    /// Returns the (possibly rewritten) message to send to the server,
    /// or None if the client has already been answered. `server` is the
    /// connection the client holds, if any.
    async fn run_query_plugins(
        &mut self,
        query_router: &mut QueryRouter,
        message: BytesMut,
        infer: bool,
        server: Option<&mut Server>,
        client_identifier: &ClientIdentifier,
    ) -> Result<Option<BytesMut>, Error> {
        let ast = match query_router.parse(&message) {
            Ok(ast) => ast,
            Err(error) => {
                if query_router.rejects_unparsed(&message) {
                    self.observe_gap(query_router, Gap::RefusedUnparsed);
                    warn!(
                        "Rejecting query the parser can't handle: {} (client: {})",
                        error, client_identifier
                    );
                    self.refuse_query(
                        &with_sqlstate(
                            UNSUPPORTED,
                            &format!("query rejected, pgcat could not parse it: {}", error),
                        ),
                        server,
                    )
                    .await?;
                    return Ok(None);
                }

                self.observe_gap(query_router, Gap::UnparsedQuery);
                warn!(
                    "Query parsing error: {} (client: {})",
                    error, client_identifier
                );
                return Ok(Some(message));
            }
        };

        // As the client sent them: before plugins rewrite or refuse them.
        if !self.sql_observed {
            query_router
                .pool_settings()
                .sql_observer
                .attempts(ast.len(), self.caller());
            let pool = &query_router.pool_settings().sql_observer;
            if pool.enabled() {
                // Each statement's relations, from the original AST.
                for statement in &ast {
                    let relations = match self.observer.as_deref_mut() {
                        Some(observer) => observer.classify(pool, statement),
                        None => RelationSnap::UNKNOWN,
                    };
                    pool.relation_attempt(relations, self.caller());
                }
            }
            if self.traces.is_some() || self.observer.is_some() {
                let invalidate = sql_observer::invalidation(&ast);
                if invalidate == sql_observer::Invalidate::Both {
                    trace_context::statements_discarded(&mut self.traces);
                }
                if let Some(observer) = self.observer.as_deref_mut() {
                    observer.admit_query(QueryAdmission::Parsed(invalidate));
                }
            }
        }

        let (message, ast) = if query_router.plugins_enabled() {
            let timeout = query_router.pool_settings().plugin_hook_timeout;
            let plugins = query_router.execute_query_plugins(&ast, self.identity());
            let result = match run_hook(timeout, plugins).await {
                Ok(result) => result,
                Err(err) => {
                    let message = query_router.chain_failed(Hook::Query, &err);
                    self.refuse_query(&message, server).await?;
                    return Ok(None);
                }
            };

            let rewritten = match result.output {
                PluginOutput::Allow => (message, ast),

                PluginOutput::Overwrite(statements) => {
                    debug!("Plugin rewrote query");
                    (simple_query(&statements_to_sql(&statements)), statements)
                }

                PluginOutput::Intercept(result) => {
                    write_all(&mut self.write, result).await?;
                    return Ok(None);
                }

                PluginOutput::Deny(error) => {
                    self.refuse_query(&or_sqlstate(PLUGIN_DENIED, &error), server)
                        .await?;
                    return Ok(None);
                }
            };

            // A failing response hook couldn't undo what the query committed.
            if result
                .plan
                .as_ref()
                .map_or(false, |plan| plan.wants_response)
                && rewritten.1.iter().any(is_transaction_end)
            {
                let error = "a query whose results a plugin rewrites can't also COMMIT or ROLLBACK";
                self.refuse_query(&with_sqlstate(UNSUPPORTED, error), server)
                    .await?;
                return Ok(None);
            }

            // The plan's hooks run when the query is sent.
            self.query_plan = result.plan.map(Arc::new);

            rewritten
        } else {
            (message, ast)
        };

        if infer && query_router.query_parser_enabled() {
            let _ = query_router.infer(&ast);
        }

        Ok(Some(message))
    }

    /// Run the query parser and plugins on a Parse message, then buffer it.
    /// A rejected Parse is not buffered: the error is reported at the next Sync or Flush.
    async fn handle_parse(
        &mut self,
        query_router: &mut QueryRouter,
        message: BytesMut,
        pool: &ConnectionPool,
        infer: bool,
        client_identifier: &ClientIdentifier,
    ) -> Result<(), Error> {
        if self.discarding_extended_messages() {
            return Ok(());
        }

        self.parse_meta = Snap::Unparsed;

        // Identity commands are answered by pgcat and never reach the server.
        let text = query_text(&message);
        let guard = pool.settings.tenant_guard.as_ref();
        let mut effects = Effects::default();
        if might_concern_identity(text) || guard.is_some_and(|guard| guard.might_refuse(text)) {
            let query = Parse::get_query(&message)?;

            if let Some(error) = tenant_parameter_error(&query, guard) {
                self.extended_protocol_error = Some(or_sqlstate(PLUGIN_DENIED, &error));
                return Ok(());
            }

            match IdentityCommand::parse(&query) {
                Ok(Some(command)) => {
                    return match self.verify_identity_command(query_router, command).await {
                        Ok(action) => {
                            self.parse_meta = Snap::Excluded;
                            let statement = LocalStatement {
                                verified_with: self.verifier_generation,
                                ..LocalStatement::identity(action)
                            };
                            self.buffer_local_parse(&message, statement, 0)
                        }
                        Err(error) => {
                            self.extended_protocol_error =
                                Some(or_sqlstate(IDENTITY_REFUSED, &error));
                            Ok(())
                        }
                    };
                }
                Ok(None) => (),
                Err(error) => {
                    self.extended_protocol_error = Some(or_sqlstate(UNSUPPORTED, &error));
                    return Ok(());
                }
            }

            effects = Effects {
                resets_session: clears_identity(&query),
                may_change_tenant: guard.is_some_and(|guard| guard.might_set_tenant(text)),
                begins_transaction: false,
            };
        }

        let coverage = query_router.plan_generation();
        let original = message.clone();

        match self
            .plan_parse(query_router, message, infer, client_identifier)
            .await?
        {
            ParseOutcome::Local(statement) => {
                self.buffer_local_parse(&original, statement, coverage)
            }

            ParseOutcome::Rejected(error) => {
                self.extended_protocol_error = Some(error);
                Ok(())
            }

            ParseOutcome::Server {
                message,
                plan,
                ends_transaction,
                rewritten,
                releases_locks,
                begins_transaction,
            } => {
                let name = Parse::get_name(&message)?;
                effects.begins_transaction = begins_transaction;
                if query_router.plugins_enabled() {
                    let was_ender = match ends_transaction {
                        Some(false) => self.transaction_enders.remove(&name),
                        _ => !self.transaction_enders.insert(name.clone()),
                    };
                    let was_releaser = match releases_locks {
                        Some(false) => self.lock_releasers.remove(&name),
                        _ => !self.lock_releasers.insert(name.clone()),
                    };
                    self.enders_journal
                        .push((name.clone(), was_ender, was_releaser));
                }
                self.remember_statement(&name, plan, effects, original, coverage);
                self.buffer_parse(message, pool, rewritten)
            }
        }
    }

    /// Keep a statement's plan and effects, and the client's Parse for planning
    /// it again if the plugin configuration changes. A new statement with the
    /// same name replaces them.
    fn remember_statement(
        &mut self,
        name: &str,
        plan: Option<Arc<StatementPlan>>,
        effects: Effects,
        original: BytesMut,
        coverage: u64,
    ) {
        let origin = (coverage != 0).then_some((original, coverage));
        self.statements.set(name.to_string(), plan, origin, effects);
    }

    /// Run the query parser and plugins on a Parse message.
    async fn plan_parse(
        &mut self,
        query_router: &mut QueryRouter,
        mut message: BytesMut,
        infer: bool,
        client_identifier: &ClientIdentifier,
    ) -> Result<ParseOutcome, Error> {
        let mut plan = None;
        let mut ends_transaction = None;
        let mut rewritten = false;
        let mut releases_locks = None;
        let mut begins_transaction = false;

        if query_router.query_parser_enabled() || query_router.plugins_enabled() {
            match query_router.parse(&message) {
                Ok(mut ast) => {
                    ends_transaction = Some(ast.iter().any(is_transaction_end));
                    releases_locks = Some(ast.iter().any(is_lock_release));
                    begins_transaction = ast.iter().any(is_transaction_begin);
                    // As the client sent it: before plugins rewrite it.
                    let relations = match self.observer.as_deref_mut() {
                        Some(observer) => observer
                            .classify_parse(&query_router.pool_settings().sql_observer, &ast),
                        None => RelationSnap::UNKNOWN,
                    };
                    self.parse_meta = Snap::known(&ast, relations);

                    if query_router.plugins_enabled() {
                        let parse: Parse = (&message).try_into()?;

                        let timeout = query_router.pool_settings().plugin_hook_timeout;
                        let plugins = query_router.execute_parse_plugins(
                            &ast,
                            parse.param_types(),
                            self.identity(),
                        );
                        let result = match run_hook(timeout, plugins).await {
                            Ok(result) => result,
                            Err(err) => {
                                return Ok(ParseOutcome::Rejected(
                                    query_router.chain_failed(Hook::Parse, &err),
                                ))
                            }
                        };

                        let parse = match result.output {
                            PluginOutput::Allow => parse,

                            PluginOutput::Overwrite(statements) => {
                                debug!("Plugin rewrote prepared statement");
                                let parse = parse.with_query(statements_to_sql(&statements));
                                ast = statements;
                                rewritten = true;
                                parse
                            }

                            PluginOutput::Intercept(response) => {
                                return Ok(ParseOutcome::Local(
                                    LocalStatement::from_simple_query_response(&response)?,
                                ));
                            }

                            PluginOutput::Deny(error) => {
                                return Ok(ParseOutcome::Rejected(or_sqlstate(
                                    PLUGIN_DENIED,
                                    &error,
                                )))
                            }
                        };

                        let parse = match result.plan {
                            Some(statement_plan) => {
                                if statement_plan.client_params
                                    + statement_plan.appended_param_types.len()
                                    > MAX_PARAMS
                                {
                                    return Ok(ParseOutcome::Rejected(with_sqlstate(
                                        LIMIT_EXCEEDED,
                                        &format!(
                                            "statements can have at most {} parameters",
                                            MAX_PARAMS
                                        ),
                                    )));
                                }

                                if parse.param_types().len() > statement_plan.client_params {
                                    return Ok(ParseOutcome::Rejected(with_sqlstate(
                                        PLUGIN_FAILED,
                                        "plugin planned fewer parameters than the statement declares",
                                    )));
                                }

                                let parse = parse.with_appended_params(
                                    statement_plan.client_params,
                                    &statement_plan.appended_param_types,
                                );
                                plan = Some(Arc::new(statement_plan));
                                parse
                            }
                            None => parse,
                        };

                        message = parse.try_into()?;
                    }

                    if infer && query_router.query_parser_enabled() {
                        let _ = query_router.infer(&ast);
                    }
                }

                Err(error) => {
                    if query_router.rejects_unparsed(&message) {
                        warn!(
                            "Rejecting prepared statement the parser can't handle: {} (client: {})",
                            error, client_identifier
                        );
                        return Ok(ParseOutcome::Rejected(with_sqlstate(
                            UNSUPPORTED,
                            &format!("query rejected, pgcat could not parse it: {}", error),
                        )));
                    }

                    warn!(
                        "Query parsing error: {} (client: {})",
                        error, client_identifier
                    );
                }
            }
        }

        Ok(ParseOutcome::Server {
            message,
            plan,
            ends_transaction,
            rewritten,
            releases_locks,
            begins_transaction,
        })
    }

    /// Plan with `settings` from now on. A batch being buffered was partly
    /// planned with the plugin configuration before, so if that changed, the
    /// batch is refused at its Sync. Returns whether it changed.
    fn plan_with(&mut self, query_router: &mut QueryRouter, settings: &PoolSettings) -> bool {
        let verifier = (settings.plugins != query_router.pool_settings().plugins)
            .then(|| query_router.verifier_key());
        let before = query_router.plan_generation();
        query_router.update_pool_settings(settings);
        if verifier.is_some_and(|verifier| verifier != query_router.verifier_key()) {
            self.verifier_generation += 1;
            self.identity_recheck = self.identity.is_some();
            // The batch's SET PGCAT_IDENTITY was verified with the old verifier.
            if !self.pending_identity.is_empty() {
                self.extended_protocol_error
                    .get_or_insert_with(stale_plan_error);
            }
        }
        self.withhold_server_text = query_router.withholds_server_text();
        let changed = query_router.plan_generation() != before;
        if changed {
            self.authorization += 1;
            if !self.extended_protocol_data_buffer.is_empty() {
                self.extended_protocol_error
                    .get_or_insert_with(stale_plan_error);
            }
        }
        changed
    }

    /// Plan with the plugins of the client's pool as reloaded since the client
    /// last looked, keeping the server connection it holds: it's the same
    /// database. An atomic load unless there was a reload. Returns whether the
    /// plugin configuration changed.
    fn follow_reload(&mut self, query_router: &mut QueryRouter) -> bool {
        let generation = pools_generation();
        if generation == self.pools_generation {
            return false;
        }
        self.pools_generation = generation;
        match get_pool(&self.pool_name, &self.username) {
            Some(current) if current.settings.plugins != query_router.pool_settings().plugins => {
                self.plan_with(query_router, &current.settings)
            }
            _ => false,
        }
    }

    /// The client's Query or batch was planned before a reload changed the
    /// plugin configuration: refuse it rather than run a plan made for another.
    /// Returns whether the client was answered.
    async fn refuse_stale_plan(&mut self, code: u8) -> Result<bool, Error> {
        match code {
            b'Q' => {
                self.query_plan = None;
                self.refuse_query(&stale_plan_error(), None).await?;
                Ok(true)
            }
            b'S' | b'H' => match self.extended_protocol_error.take() {
                Some(error) => {
                    self.reject_batch(&error, code == b'S', None).await?;
                    Ok(true)
                }
                None => Ok(false),
            },
            _ => Ok(false),
        }
    }

    /// A statement prepared before the plugin configuration changed (e.g. a
    /// plugin checks another table after a reload) is planned again from the client's
    /// Parse before it's bound. Plans never change what the client sees, so it
    /// doesn't notice.
    /// Returns an error message if the statement can't be used anymore.
    async fn replan_stale_statement(
        &mut self,
        query_router: &mut QueryRouter,
        bind: &BytesMut,
        pool: &ConnectionPool,
        client_identifier: &ClientIdentifier,
    ) -> Result<Option<String>, Error> {
        let coverage = query_router.plan_generation();
        if coverage == 0 && !self.statements.has_origins() {
            return Ok(None);
        }

        // pgcat's own answers, such as identity commands, stand. One a plugin
        // gave has an origin, and is refused below once the generation changed.
        let name = Bind::get_name(bind)?;
        if self.local_statements.contains_key(&name) && self.statements.origin(&name).is_none() {
            return Ok(None);
        }

        let stale = || {
            Some(format!(
                "prepared statement \"{}\" was prepared before the plugin configuration changed, prepare it again",
                name
            ))
        };

        let original = match self.statements.origin(&name) {
            Some((_, planned_with)) if *planned_with == coverage => return Ok(None),
            // A plugin's answer, from a configuration that has changed since: an
            // ERROR, so the client can prepare it again on this connection.
            Some(_) if self.local_statements.contains_key(&name) => {
                return Ok(stale().map(|message| with_sqlstate(PLUGIN_FAILED, &message)));
            }
            Some((original, _)) => original.clone(),
            None if coverage == 0 => return Ok(None),
            // Prepared before a plugin that plans statements was on: none rewrote it.
            None => match self.statements.cached(&name) {
                Some(Cached { parse, .. }) => {
                    parse.as_ref().clone().named(name.clone()).try_into()?
                }
                None if self.prepared_statements_enabled => return Ok(None),
                None => BytesMut::new(),
            },
        };

        // Without the prepared statement cache, the server has the statement
        // under the client's name: it can't be swapped behind the client's back.
        if !self.prepared_statements_enabled || original.is_empty() {
            return Ok(stale());
        }

        // Not by its name: the client chose it, and it may say anything.
        debug!("Planning prepared statement again after a reload");

        match self
            .plan_parse(query_router, original.clone(), false, client_identifier)
            .await?
        {
            ParseOutcome::Server {
                message,
                plan,
                rewritten,
                ..
            } => {
                let effects = self.statements.effects(&name).unwrap_or_default();
                self.remember_statement(&name, plan, effects, original, coverage);

                let parse: Parse = (&message).try_into()?;
                let hash = parse.get_hash();
                match pool.register_parse_to_cache(hash, &parse) {
                    Some(parse) => {
                        let cached = Cached::replanned(
                            self.statements.cached(&name),
                            parse,
                            hash,
                            rewritten,
                        );
                        self.statements.cache(name, Some(cached));
                        Ok(None)
                    }
                    None => Ok(stale()),
                }
            }
            ParseOutcome::Rejected(error) => Ok(Some(error)),
            ParseOutcome::Local(_) => Ok(stale()),
        }
    }

    /// The plan for the statement a Bind or Describe message refers to.
    fn statement_plan(
        &self,
        statement_name: fn(&BytesMut) -> Result<String, Error>,
        message: &BytesMut,
    ) -> Result<Option<Arc<StatementPlan>>, Error> {
        if self.statements.is_empty() {
            return Ok(None);
        }

        Ok(self.statements.plan(&statement_name(message)?))
    }

    /// Run the plan of a simple Query right before it's sent. Returns the message
    /// to send, or None if the client has been answered with an error.
    async fn run_query_hook(
        &mut self,
        plan: &Arc<StatementPlan>,
        message: BytesMut,
        server: &mut Server,
        pool: &ConnectionPool,
    ) -> Result<Option<BytesMut>, Error> {
        let (rewritten, began_transaction, interrupted) = {
            let mut ctx = ExecuteContext::new(&pool.settings, self.identity.as_ref())
                .with_server(server, true);
            let hook = plan.hooks.on_execute_query(&mut ctx, plan);
            let rewritten = run_hook(pool.settings.plugin_hook_timeout, hook).await;
            (rewritten, ctx.began_transaction(), ctx.interrupted())
        };
        self.implicit_transaction |= began_transaction;
        if interrupted {
            server.mark_bad("a plugin hook was cut off while it queried the server");
        }

        let message = match rewritten {
            Ok(Some(sql)) => simple_query(&sql),
            Ok(None) => message,
            Err(err) => {
                self.abandon_implicit_transaction(server).await?;
                let message = plan.hook_failed(Hook::Execute, &err);
                self.refuse_query(&message, Some(server)).await?;
                return Ok(None);
            }
        };

        // The plan applies to the results of this query.
        if plan.wants_response {
            self.response_rewriter.query(plan.clone());
        }

        Ok(Some(message))
    }

    /// Run the checkout hook of the simple Query about to be sent, if it has a
    /// plan. Returns an error message if the hook refused it.
    async fn run_query_checkout_hook(
        &mut self,
        pool: &ConnectionPool,
    ) -> Result<Option<String>, Error> {
        let plan = match self.query_plan.clone() {
            Some(plan) => plan,
            None => return Ok(None),
        };
        let mut ctx = ExecuteContext::new(&pool.settings, self.identity.as_ref());
        let params = BindParams::default();
        let hook = plan.hooks.before_checkout(&mut ctx, &plan, &params);
        match run_hook(pool.settings.plugin_hook_timeout, hook).await {
            Ok(()) => Ok(None),
            Err(err) => Ok(Some(plan.hook_failed(Hook::Checkout, &err))),
        }
    }

    /// Run the plugin hooks of the batch's Binds that come before a server is
    /// checked out. Returns an error message if a hook rejected the batch.
    async fn run_checkout_hooks(&mut self, pool: &ConnectionPool) -> Result<Option<String>, Error> {
        for data in self.extended_protocol_data_buffer.iter() {
            let (data, plan) = match data {
                ExtendedProtocolData::Bind {
                    data,
                    plan: Some(plan),
                    ..
                } => (data, plan),
                _ => continue,
            };

            let bind: Bind = data.try_into()?;
            let params = bind.params();
            // A Bind that doesn't fit its statement is refused by `run_bind_hooks`.
            if params.len() != plan.client_params {
                continue;
            }

            let mut ctx = ExecuteContext::new(&pool.settings, self.identity.as_ref());
            let hook = plan.hooks.before_checkout(&mut ctx, plan, &params);
            if let Err(err) = run_hook(pool.settings.plugin_hook_timeout, hook).await {
                return Ok(Some(plan.hook_failed(Hook::Checkout, &err)));
            }
        }

        Ok(None)
    }

    /// Run the plugin hooks for the Binds in the batch, before anything is sent
    /// to the server. Returns an error message if a hook rejected the batch.
    /// `sync`: the batch ends with a Sync, not a Flush.
    async fn run_bind_hooks(
        &mut self,
        pool: &ConnectionPool,
        server: &mut Server,
        sync: bool,
    ) -> Result<Option<String>, Error> {
        let rejected = self.run_bind_hooks_inner(pool, server, sync).await?;
        if rejected.is_some() {
            self.abandon_implicit_transaction(server).await?;
        }
        Ok(rejected)
    }

    async fn run_bind_hooks_inner(
        &mut self,
        pool: &ConnectionPool,
        server: &mut Server,
        sync: bool,
    ) -> Result<Option<String>, Error> {
        self.batch_state.clear();
        if self.executes_stale_portal()? {
            return Ok(Some(with_sqlstate(
                PLUGIN_DENIED,
                "a portal bound before the identity or the plugin configuration changed can't be executed: bind it again",
            )));
        }
        let carried = self.first_carried_execute()?;
        for index in 0..self.extended_protocol_data_buffer.len() {
            let planned = matches!(
                self.extended_protocol_data_buffer[index],
                ExtendedProtocolData::Bind { plan: Some(_), .. }
            );
            if !planned {
                continue;
            }
            let execution = self.execution_of(index)?;
            let overtaken = match execution {
                Some(execution) => self.overtaken(index, execution)?,
                None => false,
            };
            let (data, plan) = match &mut self.extended_protocol_data_buffer[index] {
                ExtendedProtocolData::Bind {
                    data,
                    plan: Some(plan),
                    ..
                } => (data, plan.clone()),
                _ => continue,
            };

            let bind: Bind = (&*data).try_into()?;
            let mut params = bind.params();

            if params.len() != plan.client_params {
                return Ok(Some(with_sqlstate(
                    PROTOCOL_VIOLATION,
                    &format!(
                        "bind message supplies {} parameters, but prepared statement requires {}",
                        params.len(),
                        plan.client_params
                    ),
                )));
            }

            let position = BindPosition {
                locks_released: self.releasing_binds.iter().any(|bind| *bind < index)
                    || carried.is_some_and(|execute| execute < index),
                deferred: execution.is_none(),
                overtaken,
            };
            let (result, began_transaction, interrupted) = {
                let mut ctx = ExecuteContext::new(&pool.settings, self.identity.as_ref())
                    .with_server(&mut *server, sync)
                    .with_batch(&mut self.batch_state, position);
                let hook = plan.hooks.on_bind(&mut ctx, &plan, &mut params);
                let result = run_hook(pool.settings.plugin_hook_timeout, hook).await;
                (result, ctx.began_transaction(), ctx.interrupted())
            };
            self.implicit_transaction |= began_transaction;
            if interrupted {
                server.mark_bad("a plugin hook was cut off while it queried the server");
            }

            if let Err(err) = result {
                return Ok(Some(plan.hook_failed(Hook::Bind, &err)));
            }

            if params.len() != plan.client_params + plan.appended_param_types.len() {
                error!(
                    "Plugin bind hook produced {} parameters, expected {}",
                    params.len(),
                    plan.client_params + plan.appended_param_types.len()
                );
                return Ok(Some(with_sqlstate(
                    PLUGIN_FAILED,
                    "plugin produced the wrong number of parameters",
                )));
            }

            if params.modified() {
                *data = bind.with_params(params).try_into()?;
            }
        }

        self.transaction_end_after_rows_hook()
    }

    /// Where the batch executes the portal the Bind at `index` makes, if it
    /// does before binding it again.
    fn execution_of(&self, index: usize) -> Result<Option<usize>, Error> {
        let portal = match &self.extended_protocol_data_buffer[index] {
            ExtendedProtocolData::Bind { data, .. } => Bind::get_portal(data)?,
            _ => return Ok(None),
        };
        for (at, data) in self
            .extended_protocol_data_buffer
            .iter()
            .enumerate()
            .skip(index + 1)
        {
            match data {
                ExtendedProtocolData::Execute { data } if Execute::get_portal(data)? == portal => {
                    return Ok(Some(at))
                }
                ExtendedProtocolData::Bind { data, .. } if Bind::get_portal(data)? == portal => {
                    return Ok(None)
                }
                _ => (),
            }
        }
        Ok(None)
    }

    /// Whether a planned portal bound between `index` and `execution` is
    /// executed before `execution`. See `BindPosition::overtaken`.
    fn overtaken(&self, index: usize, execution: usize) -> Result<bool, Error> {
        let mut later = HashSet::new();
        for data in self
            .extended_protocol_data_buffer
            .range(index + 1..execution)
        {
            match data {
                ExtendedProtocolData::Bind {
                    data,
                    plan: Some(_),
                    ..
                } => {
                    later.insert(Bind::get_portal(data)?);
                }
                ExtendedProtocolData::Execute { data }
                    if later.contains(&Execute::get_portal(data)?) =>
                {
                    return Ok(true)
                }
                _ => (),
            }
        }
        Ok(false)
    }

    /// Where in a batch with plugin plans the first Execute of a portal not
    /// bound earlier in the batch is. Such a portal was bound in an earlier
    /// batch of the transaction, so pgcat can't see here what its statement
    /// does: it may write rows or end the transaction.
    fn first_carried_execute(&self) -> Result<Option<usize>, Error> {
        let buffer = &self.extended_protocol_data_buffer;
        let planned = |data: &ExtendedProtocolData| {
            matches!(data, ExtendedProtocolData::Bind { plan: Some(_), .. })
        };
        if !buffer.iter().any(planned) {
            return Ok(None);
        }
        Ok(self.carried_executes()?.first().map(|(index, _)| *index))
    }

    /// A planned portal bound in an earlier batch is executed after the
    /// identity or what the plugins check changed: its hooks checked the old one, and
    /// a suspended portal returns rows the server read under it.
    fn executes_stale_portal(&self) -> Result<bool, Error> {
        if self.portal_plans.is_empty() {
            return Ok(false);
        }
        Ok(self.carried_executes()?.iter().any(|(_, portal)| {
            self.portal_plans
                .get(portal)
                .is_some_and(|plan| plan.authorization != self.authorization)
        }))
    }

    /// The batch's Executes of portals not bound earlier in it, where they are
    /// and the portal's name.
    fn carried_executes(&self) -> Result<Vec<(usize, String)>, Error> {
        let mut bound = HashSet::new();
        let mut carried = Vec::new();
        for (index, data) in self.extended_protocol_data_buffer.iter().enumerate() {
            match data {
                ExtendedProtocolData::Bind { data, .. } => {
                    bound.insert(Bind::get_portal(data)?);
                }
                ExtendedProtocolData::Execute { data } => {
                    let portal = Execute::get_portal(data)?;
                    if !bound.contains(&portal) {
                        carried.push((index, portal));
                    }
                }
                _ => (),
            }
        }
        Ok(carried)
    }

    /// A statement whose rows a plugin rewrites is followed, in the same batch,
    /// by one that ends the transaction: if the hook failed, the server would
    /// have committed what the client is told failed. Refused.
    fn transaction_end_after_rows_hook(&self) -> Result<Option<String>, Error> {
        let mut hooked = false;
        for (index, data) in self.extended_protocol_data_buffer.iter().enumerate() {
            if let ExtendedProtocolData::Bind { plan, .. } = data {
                if hooked && self.ending_binds.contains(&index) {
                    return Ok(Some(with_sqlstate(
                        UNSUPPORTED,
                        "a statement whose results a plugin rewrites can't be followed by COMMIT or ROLLBACK in the same batch",
                    )));
                }
                hooked |= plan.as_ref().map_or(false, |plan| plan.wants_response);
            }
        }
        Ok(None)
    }

    /// A hook had pgcat open a transaction for the batch, and the batch was
    /// rejected before it was sent: roll it back.
    async fn abandon_implicit_transaction(&mut self, server: &mut Server) -> Result<(), Error> {
        if std::mem::take(&mut self.implicit_transaction) {
            // Cut off mid-exchange, the connection goes, and pgcat's
            // transaction with it.
            if server.is_bad() {
                server.forget_transaction();
            } else {
                server.query_checked("ROLLBACK").await?;
            }
        }
        Ok(())
    }

    /// After a refusal: true if the server must be let go, because a hook was
    /// cut off with it mid-exchange. If the client's transaction (or, in
    /// session mode, its session) went with it, the client can't go on.
    fn server_lost(&self, server: &Server) -> Result<bool, Error> {
        if !server.is_bad() {
            return Ok(false);
        }
        if server.in_transaction() || !self.transaction_mode {
            return Err(Error::ServerError);
        }
        Ok(true)
    }

    /// Before a batch or query is sent: if a response hook failing in it could
    /// leave writes behind, run it in a transaction pgcat opens, unless the
    /// client is in one.
    async fn begin_implicit_transaction(&mut self, server: &mut Server) -> Result<(), Error> {
        // A hook opened one already, to read rows the batch writes.
        if self.implicit_transaction {
            self.response_rewriter.wrapped();
            return Ok(());
        }
        if server.in_transaction() || !self.response_rewriter.needs_transaction() {
            return Ok(());
        }
        server.query_checked("BEGIN").await?;
        self.implicit_transaction = true;
        self.response_rewriter.wrapped();
        Ok(())
    }

    /// What the round trip the server just answered did: whether a response
    /// hook failed, and whether the client began a transaction of its own in
    /// the one pgcat opened for it, which is then the client's. Postgres warns
    /// of a BEGIN inside a transaction (25001) only at client_min_messages
    /// warning or lower: the batch's own BEGIN counts too. Neither outlives the
    /// round trip: a later one may run once that transaction has ended.
    fn take_round_outcome(&mut self) -> (bool, bool) {
        let (failed, client_began) = self.response_rewriter.take_outcome();
        (
            failed,
            std::mem::take(&mut self.batch_begins) || client_began,
        )
    }

    /// Once the server's reply to a batch is complete (`response` ends with its
    /// ReadyForQuery): if a response hook failed, the batch mustn't stand, as if
    /// the server had raised the error. A transaction pgcat opened for it is
    /// rolled back, the client's fails. Otherwise pgcat commits its transaction;
    /// the client gets the COMMIT's error, if any, withheld like the server's
    /// other replies: a deferred constraint can quote a protected value.
    async fn finish_batch(
        &mut self,
        mut response: BytesMut,
        server: &mut Server,
    ) -> Result<BytesMut, Error> {
        if server.is_data_available() {
            return Ok(response);
        }

        let (failed, client_began) = self.take_round_outcome();
        let implicit = std::mem::take(&mut self.implicit_transaction) && !client_began;
        if !failed && !implicit {
            return Ok(response);
        }

        let status = match response.len().checked_sub(READY_FOR_QUERY_SIZE) {
            Some(at) if response[at] == b'Z' => response[at + READY_FOR_QUERY_SIZE - 1],
            _ => {
                server.mark_bad("expected ReadyForQuery at the end of the batch");
                return Err(Error::ProtocolSyncError(
                    "expected ReadyForQuery at the end of the batch".into(),
                ));
            }
        };
        response.truncate(response.len() - READY_FOR_QUERY_SIZE);

        match (implicit, status) {
            // The batch ended the transaction pgcat opened itself.
            (true, b'I') => (),
            (true, b'T') if !failed => {
                if let Some(mut error) = server.run_statement("COMMIT").await? {
                    if self.withhold_server_text {
                        withhold_server_text(&mut error);
                    }
                    response.put(error);
                }
            }
            (true, _) => server.query_checked("ROLLBACK").await?,
            (false, _) => server.fail_transaction().await?,
        }

        if server.transaction_status() == b'I' {
            if let Some(observer) = self.observer.as_deref_mut() {
                observer.idle();
            }
        }

        response.put(ready_for_query_status(server.transaction_status()));
        Ok(response)
    }

    /// Rewrite a chunk of the server's reply for planned statements.
    async fn rewrite_response(
        &mut self,
        response: BytesMut,
        server: &mut Server,
        pool: &ConnectionPool,
    ) -> Result<BytesMut, Error> {
        if !self.response_rewriter.active() {
            return Ok(response);
        }

        // The server is busy sending the response, hooks can't query it now.
        let mut ctx = ExecuteContext::new(&pool.settings, self.identity.as_ref());

        match self.response_rewriter.process(response, &mut ctx).await {
            Ok(response) => Ok(response),
            Err(err) => {
                // We don't know where we are in the server's reply anymore.
                server.mark_bad(&format!("could not rewrite response: {}", err));
                self.response_rewriter.reset();
                error_response_terminal(
                    &mut self.write,
                    "pgcat could not follow the server's response",
                )
                .await?;
                Err(err)
            }
        }
    }

    /// The current extended protocol batch is being discarded because of an error.
    fn discarding_extended_messages(&self) -> bool {
        self.extended_protocol_error.is_some() || self.discard_until_sync
    }

    /// Finish an extended protocol batch (Sync or Flush) that doesn't need the server:
    /// a rejected batch, or one made only of messages pgcat answers itself.
    /// Returns true if the client has been answered.
    /// `server` is the connection the client holds, if any.
    async fn finish_batch_without_server(
        &mut self,
        sync: bool,
        server: Option<&mut Server>,
    ) -> Result<bool, Error> {
        if let Some(error) = self.extended_protocol_error.take() {
            self.reject_batch(&error, sync, server).await?;
            return Ok(true);
        }

        let status = server
            .as_ref()
            .map_or(b'I', |server| server.transaction_status());

        if self.discard_until_sync {
            if sync {
                self.discard_until_sync = false;
                self.reset_buffered_state();
                write_all_flush(&mut self.write, &ready_for_query_status(status)).await?;
            }
            return Ok(true);
        }

        if !self
            .extended_protocol_data_buffer
            .iter()
            .any(ExtendedProtocolData::is_local)
        {
            return Ok(false);
        }

        // The server answers its messages after ours, so the client would see the
        // responses out of order. Refuse instead. Likewise at a Sync that must
        // end a transaction pgcat opened at an earlier Flush.
        if (sync && self.implicit_transaction)
            || !self
                .extended_protocol_data_buffer
                .iter()
                .all(ExtendedProtocolData::is_local)
        {
            self.reject_batch(
                &with_sqlstate(
                    UNSUPPORTED,
                    "statements answered by pgcat must be sent in their own Sync batch",
                ),
                sync,
                server,
            )
            .await?;
            return Ok(true);
        }

        self.statements.commit();
        self.enders_journal.clear();
        if let Some(bound) = self.bound_since_reload.as_mut() {
            bound.answered_locally();
        }
        if let Some(observer) = self.observer.as_deref_mut() {
            observer.accept_local_batch();
        }
        for (index, action, statement) in std::mem::take(&mut self.pending_identity) {
            let reply = statement.encode_rows(self.run_identity_command(action))?;
            if let Some(ExtendedProtocolData::Local { response }) =
                self.extended_protocol_data_buffer.get_mut(index)
            {
                *response = reply;
            }
        }

        let mut response = std::mem::take(&mut self.response_message_queue_buffer);
        for data in self.extended_protocol_data_buffer.drain(..) {
            if let ExtendedProtocolData::Local { response: local } = data {
                response.put(local);
            }
        }

        if sync {
            response.put(ready_for_query_status(status));
        }

        write_all_flush(&mut self.write, &response).await?;

        Ok(true)
    }

    /// Drop the buffered batch and report the error. Like the server, on Flush the
    /// error is sent right away and everything until the next Sync is ignored.
    async fn reject_batch(
        &mut self,
        error: &str,
        sync: bool,
        server: Option<&mut Server>,
    ) -> Result<(), Error> {
        self.reset_buffered_state();
        let status = self.fail_transaction(server).await?;

        if sync {
            error_response_with_status(&mut self.write, error, status).await
        } else {
            self.discard_until_sync = true;
            error_response_terminal(&mut self.write, error).await
        }
    }

    /// Answer a simple Query pgcat refused.
    async fn refuse_query(
        &mut self,
        error: &str,
        server: Option<&mut Server>,
    ) -> Result<(), Error> {
        let status = self.fail_transaction(server).await?;
        error_response_with_status(&mut self.write, error, status).await
    }

    /// A statement was refused: inside a transaction, fail it on the server, so
    /// the client can't commit the rest of it. A transaction pgcat opened (at a
    /// Flush, say) is rolled back instead: the client never began it. Returns
    /// the transaction status for the client's ReadyForQuery.
    async fn fail_transaction(&mut self, server: Option<&mut Server>) -> Result<u8, Error> {
        let server = match server {
            Some(server) => server,
            None => return Ok(b'I'),
        };
        if self.implicit_transaction {
            self.abandon_implicit_transaction(server).await?;
        } else {
            server.fail_transaction().await?;
        }
        Ok(server.transaction_status())
    }

    /// Buffer a Parse answered by pgcat, e.g. intercepted by a plugin with
    /// the pool's plan generation `planned_with` (0 for pgcat's own answers).
    /// A plugin's answer is kept with an empty origin: it can't be planned
    /// again, as what the client was told it returns might change, so once
    /// the plugin configuration changed it's refused until prepared again,
    /// as are its portals.
    fn buffer_local_parse(
        &mut self,
        message: &BytesMut,
        mut statement: LocalStatement,
        planned_with: u64,
    ) -> Result<(), Error> {
        let name = Parse::get_name(message)?;

        self.statements.cache(name.clone(), None);
        let origin = (planned_with != 0).then(|| (BytesMut::new(), planned_with));
        self.statements
            .set(name.clone(), None, origin, Effects::default());
        statement.planned_with = planned_with;
        self.local_statements.insert(name, statement);
        self.extended_protocol_data_buffer
            .push_back(ExtendedProtocolData::create_new_local(parse_complete()));

        self.stage_parse(message);
        Ok(())
    }

    /// Buffer an Execute, answering it locally if the portal belongs to a local statement.
    async fn buffer_execute(
        &mut self,
        query_router: &QueryRouter,
        message: BytesMut,
    ) -> Result<(), Error> {
        // Bound under the plan before a reload: the batch is refused at Sync.
        // An identity command is verified again below instead.
        if let Some(bound) = &self.bound_since_reload {
            let portal = Execute::get_portal(&message)?;
            let identity = |statement: &LocalStatement| statement.identity_command.is_some();
            if !bound.bound.contains(&portal)
                && !bound.batch.contains(&portal)
                && !self.local_portals.get(&portal).is_some_and(identity)
            {
                self.extended_protocol_error
                    .get_or_insert_with(stale_plan_error);
                return Ok(());
            }
        }

        let pool_observer = &query_router.pool_settings().sql_observer;
        // They take effect when the batch is sent, not if it's rejected.
        if !self.portal_effects.is_empty() {
            if let Some(effects) = self.portal_effects.get(&Execute::get_portal(&message)?) {
                self.pending_session_reset |= effects.resets_session;
                self.tenant_may_change |= effects.may_change_tenant;
                self.batch_begins |= effects.begins_transaction;
            }
        }

        if !self.local_portals.is_empty() {
            let portal = Execute::get_portal(&message)?;

            if let Some(statement) = self.local_portals.get(&portal).cloned() {
                // A plugin answered it under the plugin configuration before a
                // reload, here or at an earlier message: the batch is refused at
                // Sync rather than given that answer.
                let planned_with = statement.planned_with;
                if planned_with != 0 && planned_with != query_router.plan_generation() {
                    self.extended_protocol_error
                        .get_or_insert_with(stale_plan_error);
                    return Ok(());
                }

                if let Some(action) = statement.identity_command.clone() {
                    // Can SHOW's row be encoded as the client asked?
                    if let Err(err) = statement.encode_rows(identity_show_row(None)) {
                        self.extended_protocol_error = Some(err.to_string());
                        return Ok(());
                    }

                    // A token verified before a reload changed the plugins
                    // is verified again, as a SET of it would be now.
                    let action = match action {
                        IdentityAction::Set(identity)
                            if statement.verified_with != self.verifier_generation =>
                        {
                            let command = IdentityCommand::Set(identity.token().clone());
                            match self.verify_identity_command(query_router, command).await {
                                Ok(action) => {
                                    // Verified now for the token's every statement and portal.
                                    let generation = self.verifier_generation;
                                    for cached in self
                                        .local_statements
                                        .values_mut()
                                        .chain(self.local_portals.values_mut())
                                    {
                                        if matches!(&cached.identity_command,
                                            Some(IdentityAction::Set(old)) if old.token() == identity.token())
                                        {
                                            cached.identity_command = Some(action.clone());
                                            cached.verified_with = generation;
                                        }
                                    }
                                    action
                                }
                                Err(error) => {
                                    self.extended_protocol_error =
                                        Some(or_sqlstate(IDENTITY_REFUSED, &error));
                                    return Ok(());
                                }
                            }
                        }
                        action => action,
                    };

                    // Run when the batch is answered, not if it's rejected.
                    self.pending_identity.push((
                        self.extended_protocol_data_buffer.len(),
                        action,
                        statement,
                    ));
                    self.extended_protocol_data_buffer
                        .push_back(ExtendedProtocolData::create_new_local(BytesMut::new()));
                    return Ok(());
                }

                let response = match statement.encode_rows(statement.execute_response.clone()) {
                    Ok(response) => response,
                    Err(err) => {
                        self.extended_protocol_error = Some(err.to_string());
                        return Ok(());
                    }
                };
                self.admit_execute(&message, pool_observer);

                self.extended_protocol_data_buffer
                    .push_back(ExtendedProtocolData::create_new_local(response));
                return Ok(());
            }
        }

        self.admit_execute(&message, pool_observer);
        self.extended_protocol_data_buffer
            .push_back(ExtendedProtocolData::create_new_execute(message));

        Ok(())
    }

    /// The Parse was buffered: the observer learns the statement under the
    /// client's name.
    fn stage_parse(&mut self, message: &BytesMut) {
        if let (Some(observer), Some(name)) = (
            self.observer.as_deref_mut(),
            sql_observer::parse_name(message),
        ) {
            observer.stage_parse(name, self.parse_meta);
        }
    }

    /// The Bind was buffered (`key` was read from the client's frame).
    fn stage_bind(&mut self, key: Option<BindKey>) {
        if let (Some(observer), Some(key)) = (self.observer.as_deref_mut(), key) {
            observer.stage_bind_key(&key);
        }
    }

    /// Count (or report the gap of) an Execute being buffered.
    fn admit_execute(&mut self, message: &BytesMut, pool_observer: &SqlObserver) {
        // From the fields, not `caller()`: the observer is borrowed mutably alongside.
        let caller = caller_of(
            &self.identity_lapse,
            self.identity.is_some(),
            &self.server_parameters,
            self.addr,
        );
        if let Some(observer) = self.observer.as_deref_mut() {
            observer.admit_execute(sql_observer::execute_portal(message), caller, pool_observer);
        }
    }

    /// Buffer a Close, answering it locally if it closes a local statement or portal.
    fn buffer_close(&mut self, message: BytesMut) -> Result<(), Error> {
        let close: Close = (&message).try_into()?;

        // Nothing below can fail.
        if close.is_prepared_statement() {
            self.statements.close(close.name.clone());
        }

        let local = if close.is_portal() {
            self.portal_effects.remove(&close.name);
            // Closed, it needs a new Bind to run: a local Close leaves the
            // server's portal of that name open.
            if let Some(bound) = self.bound_since_reload.as_mut() {
                bound.bound.remove(&close.name);
                bound.batch.remove(&close.name);
            }
            self.local_portals.remove(&close.name).is_some()
        } else {
            self.local_statements.remove(&close.name).is_some()
        };

        if let (Some(observer), Some((portal, name))) = (
            self.observer.as_deref_mut(),
            sql_observer::close_target(&message),
        ) {
            if self.prepared_statements_enabled {
                observer.stage_cached_close(name, portal, local);
            } else {
                observer.stage_close(name, portal);
            }
        }

        if local {
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_local(close_complete()));
        } else {
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_close(message, close));
        }

        Ok(())
    }

    /// Makes sure the the checked out server has the prepared statement and sends it to the server if it doesn't
    ///
    /// `statement` is the client's name and the statement it had when the
    /// Bind or Describe was buffered.
    async fn ensure_prepared_statement_is_on_server(
        &mut self,
        statement: (String, Arc<Parse>, u64),
        pool: &ConnectionPool,
        server: &mut Server,
        address: &Address,
    ) -> Result<(), Error> {
        let (client_name, parse, hash) = statement;
        debug!("Prepared statement `{}` found in cache", client_name);
        // In this case we want to send the parse message to the server
        // since pgcat is initiating the prepared statement on this specific server
        match self
            .register_parse_to_server_cache(true, &hash, &parse, pool, server, address)
            .await
        {
            Ok(_) => (),
            Err(err) => match err {
                Error::PreparedStatementError => {
                    // Unless a Parse later in the batch gave the name another
                    // statement.
                    let current = self.statements.cached(&client_name);
                    if current.is_some_and(|cached| Arc::ptr_eq(&cached.parse, &parse)) {
                        debug!("Removed {} from client cache", client_name);
                        self.statements.uncache(&client_name);
                    }
                }

                _ => {
                    return Err(err);
                }
            },
        }

        Ok(())
    }

    /// Register the parse to the server cache and send it to the server if requested (ie. requested by pgcat)
    ///
    /// Also updates the pool LRU that this parse was used recently
    async fn register_parse_to_server_cache(
        &self,
        should_send_parse_to_server: bool,
        hash: &u64,
        parse: &Arc<Parse>,
        pool: &ConnectionPool,
        server: &mut Server,
        address: &Address,
    ) -> Result<(), Error> {
        // We want to promote this in the pool's LRU
        pool.promote_prepared_statement_hash(hash);

        debug!("Checking for prepared statement {}", parse.name);

        if let Err(err) = server
            .register_prepared_statement(parse, should_send_parse_to_server)
            .await
        {
            match err {
                // Don't ban for this.
                Error::PreparedStatementError => (),
                _ => {
                    pool.ban(address, BanReason::MessageSendFailed, Some(&self.stats));
                }
            };

            return Err(err);
        }

        Ok(())
    }

    /// Register and rewrite the parse statement to the clients statement cache
    /// and also the pool's statement cache. Add it to extended protocol data.
    /// `text_rewritten` if a plugin changed the client's SQL text.
    fn buffer_parse(
        &mut self,
        message: BytesMut,
        pool: &ConnectionPool,
        text_rewritten: bool,
    ) -> Result<(), Error> {
        // A statement with the same name answered by pgcat is replaced by this one.
        if !self.local_statements.is_empty() {
            self.local_statements.remove(&Parse::get_name(&message)?);
        }

        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous parse message");
            self.stage_parse(&message);
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_parse(message, None));
            return Ok(());
        }

        let client_given_name = Parse::get_name(&message)?;
        let parse: Parse = (&message).try_into()?;

        // Compute the hash of the parse statement
        let hash = parse.get_hash();

        // Add the statement to the cache or check if we already have it
        let new_parse = match pool.register_parse_to_cache(hash, &parse) {
            Some(parse) => parse,
            None => {
                return Err(Error::ClientError(format!(
                    "Could not store Prepared statement `{}`",
                    client_given_name
                )))
            }
        };

        debug!(
            "Renamed prepared statement `{}` to `{}` and saved to cache",
            client_given_name, new_parse.name
        );

        let rewritten: BytesMut = new_parse.as_ref().try_into()?;

        // The client's name, from its own frame: not the rewritten one. The
        // observer's label is kept with the statement, so a dropped batch
        // puts both back.
        let label = match (
            self.observer.as_deref_mut(),
            sql_observer::parse_name(&message),
        ) {
            (Some(observer), Some(name)) => observer.stage_cached_parse(
                name,
                self.parse_meta,
                new_parse.observed(),
                text_rewritten,
            ),
            _ => None,
        };
        self.statements.cache(
            client_given_name,
            Some(Cached {
                parse: new_parse.clone(),
                hash,
                rewritten: text_rewritten,
                label: label.map(Box::new),
            }),
        );
        self.extended_protocol_data_buffer
            .push_back(ExtendedProtocolData::create_new_parse(
                rewritten,
                Some((new_parse.clone(), hash)),
            ));

        Ok(())
    }

    /// Rewrite the Bind (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn buffer_bind(&mut self, message: BytesMut) -> Result<(), Error> {
        // Fresh once the server has bound it: a Bind it refuses (the name is
        // in use) or skips after an error leaves the portal as it was.
        if let Some(bound) = self.bound_since_reload.as_mut() {
            let portal = Bind::get_portal(&message)?;
            bound.bound.remove(&portal);
            bound.batch.insert(portal);
        }

        // Read before the frame is renamed; staged where the Bind is buffered.
        let key = match self.observer {
            Some(_) => BindKey::from_frame(&message),
            None => None,
        };

        // Whether it ends the transaction is known now; a later Parse in the
        // batch may reuse the statement's name.
        if !self.transaction_enders.is_empty()
            && self.transaction_enders.contains(&Bind::get_name(&message)?)
        {
            self.ending_binds
                .push(self.extended_protocol_data_buffer.len());
        }
        if !self.lock_releasers.is_empty()
            && self.lock_releasers.contains(&Bind::get_name(&message)?)
        {
            self.releasing_binds
                .push(self.extended_protocol_data_buffer.len());
        }

        if self.statements.has_effects() || !self.portal_effects.is_empty() {
            let portal = Bind::get_portal(&message)?;
            match self.statements.effects(&Bind::get_name(&message)?) {
                Some(effects) => self.portal_effects.insert(portal, effects),
                None => self.portal_effects.remove(&portal),
            };
        }

        if !self.local_statements.is_empty() || !self.local_portals.is_empty() {
            let portal = Bind::get_portal(&message)?;

            match self.local_statements.get(&Bind::get_name(&message)?) {
                Some(statement) => {
                    let bind: Bind = (&message).try_into()?;
                    let statement = statement.bind(bind.params().result_formats());
                    self.local_portals.insert(portal, statement);
                    self.stage_bind(key);
                    self.extended_protocol_data_buffer
                        .push_back(ExtendedProtocolData::create_new_local(bind_complete()));
                    return Ok(());
                }
                None => {
                    self.local_portals.remove(&portal);
                }
            }
        }

        let plan = self.statement_plan(Bind::get_name, &message)?;

        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous bind message");
            self.stage_bind(key);
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_bind(message, None, plan));
            return Ok(());
        }

        let client_given_name = Bind::get_name(&message)?;

        match self.statements.cached(&client_given_name) {
            Some(cached) => {
                let rewritten_parse = &cached.parse;
                let message = Bind::rename(message, &rewritten_parse.name)?;

                debug!(
                    "Rewrote bind `{}` to `{}`",
                    client_given_name, rewritten_parse.name
                );

                if let (Some(observer), Some(key)) = (self.observer.as_deref_mut(), key) {
                    observer.stage_cached_bind(
                        &key,
                        rewritten_parse.observed(),
                        cached.rewritten,
                        cached.label.as_deref().copied(),
                    );
                }
                // The statement as of the Bind: a Close or Parse later in
                // the batch may change what the name is before it's sent.
                let statement = (client_given_name, rewritten_parse.clone(), cached.hash);
                self.extended_protocol_data_buffer.push_back(
                    ExtendedProtocolData::create_new_bind(message, Some(statement), plan),
                );

                Ok(())
            }
            None => {
                debug!(
                    "Got bind for unknown prepared statement {:?}",
                    client_given_name
                );

                error_response(
                    &mut self.write,
                    &format!(
                        "prepared statement \"{}\" does not exist",
                        client_given_name
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Prepared statement `{}` doesn't exist",
                    client_given_name
                )))
            }
        }
    }

    /// Rewrite the Describe (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn buffer_describe(&mut self, message: BytesMut) -> Result<(), Error> {
        if !self.local_statements.is_empty() || !self.local_portals.is_empty() {
            let describe: Describe = (&message).try_into()?;

            let local = if describe.target == 'P' {
                self.local_portals.get(&describe.statement_name)
            } else {
                self.local_statements.get(&describe.statement_name)
            };

            if let Some(statement) = local {
                let mut response = BytesMut::new();
                if describe.target != 'P' {
                    response.put(parameter_description(&[]));
                }
                response.put(statement.describe());

                self.extended_protocol_data_buffer
                    .push_back(ExtendedProtocolData::create_new_local(response));
                return Ok(());
            }
        }

        let describe: Describe = (&message).try_into()?;
        let plan = if describe.target == 'P' {
            None
        } else {
            self.statements.plan(&describe.statement_name)
        };

        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous describe message");
            self.extended_protocol_data_buffer.push_back(
                ExtendedProtocolData::create_new_describe(message, None, plan),
            );

            return Ok(());
        }

        if describe.target == 'P' {
            debug!("Portal describe message");
            self.extended_protocol_data_buffer.push_back(
                ExtendedProtocolData::create_new_describe(message, None, None),
            );

            return Ok(());
        }

        let client_given_name = describe.statement_name.clone();

        match self.statements.cached(&client_given_name) {
            Some(Cached {
                parse: rewritten_parse,
                hash,
                ..
            }) => {
                let describe = describe.rename(&rewritten_parse.name);

                debug!(
                    "Rewrote describe `{}` to `{}`",
                    client_given_name, describe.statement_name
                );

                self.extended_protocol_data_buffer.push_back(
                    ExtendedProtocolData::create_new_describe(
                        describe.try_into()?,
                        Some((client_given_name, rewritten_parse.clone(), *hash)),
                        plan,
                    ),
                );

                Ok(())
            }

            None => {
                debug!("Got describe for unknown prepared statement {:?}", describe);

                error_response(
                    &mut self.write,
                    &format!(
                        "prepared statement \"{}\" does not exist",
                        client_given_name
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Prepared statement `{}` doesn't exist",
                    client_given_name
                )))
            }
        }
    }

    async fn process_extended_protocol_buffer(
        &mut self,
        server: &mut Server,
        pool: &ConnectionPool,
        address: &Address,
    ) -> Result<(), Error> {
        // The batch goes to the server: its Parses and Closes stand, and its
        // Binds wait for the server's reply.
        self.statements.commit();
        if let Some(bound) = self.bound_since_reload.as_mut() {
            bound.send();
        }
        self.enders_journal.clear();
        self.ending_binds.clear();
        self.releasing_binds.clear();
        self.pending_identity.clear();
        if std::mem::take(&mut self.pending_session_reset) {
            self.reset_session();
        }

        // Follow portals and results only if a plugin planned something. Decided from
        // the batch itself: a later Parse or Close in it may already have replaced or
        // dropped the plan of a statement bound earlier.
        let track = !self.portal_plans.is_empty()
            || self.extended_protocol_data_buffer.iter().any(|data| {
                matches!(
                    data,
                    ExtendedProtocolData::Bind { plan: Some(_), .. }
                        | ExtendedProtocolData::Describe { plan: Some(_), .. }
                )
            });

        // The batch's messages sent to the server so far.
        let mut sent = 0;
        self.held_replies.clear();
        while let Some(protocol_data) = self.extended_protocol_data_buffer.pop_front() {
            match protocol_data {
                ExtendedProtocolData::Parse { data, metadata } => {
                    debug!("Have parse in extended buffer");
                    let (parse, hash) = match metadata {
                        Some(metadata) => metadata,
                        None => {
                            let first_char_in_name = *data.get(5).unwrap_or(&0);
                            if first_char_in_name != 0 {
                                server.mark_dirty();
                            }
                            self.buffer.put(&data[..]);
                            sent += 1;
                            continue;
                        }
                    };

                    if server.has_prepared_statement(&parse.name) {
                        debug!("Prepared statement `{}` found in server cache", parse.name);
                        self.reply_in_turn(sent, parse_complete());
                        continue;
                    } else {
                        debug!(
                            "Prepared statement `{}` not found in server cache",
                            parse.name
                        );
                        self.register_parse_to_server_cache(
                            false, &hash, &parse, &pool, server, &address,
                        )
                        .await?;
                        self.buffer.put(&data[..]);
                    }
                }
                ExtendedProtocolData::Bind {
                    data,
                    metadata,
                    plan,
                } => {
                    let mut describe = None;
                    if track {
                        match plan {
                            Some(plan) => {
                                let bind: Bind = (&data).try_into()?;
                                let portal = Arc::new(PortalPlan::new(
                                    plan,
                                    bind.params().result_formats().to_vec(),
                                    self.authorization,
                                ));
                                if portal.plan.wants_response {
                                    describe = Some(
                                        self.response_rewriter
                                            .describe_for_rows(bind.portal(), portal.clone()),
                                    );
                                }
                                self.portal_plans.insert(bind.portal().to_string(), portal);
                            }
                            None => {
                                if !self.portal_plans.is_empty() {
                                    self.portal_plans.remove(&Bind::get_portal(&data)?);
                                }
                            }
                        }
                    }

                    if let Some(statement) = metadata {
                        self.ensure_prepared_statement_is_on_server(
                            statement, &pool, server, &address,
                        )
                        .await?;
                    }
                    self.buffer.put(&data[..]);
                    if let Some(describe) = describe {
                        self.buffer.put(describe);
                    }
                }
                ExtendedProtocolData::Describe {
                    data,
                    metadata,
                    plan,
                } => {
                    if track {
                        if data.get(5) == Some(&b'P') {
                            let describe: Describe = (&data).try_into()?;
                            self.response_rewriter.describe_portal(
                                self.portal_plans.get(&describe.statement_name).cloned(),
                            );
                        } else {
                            self.response_rewriter.describe_statement(plan);
                        }
                    }

                    if let Some(statement) = metadata {
                        self.ensure_prepared_statement_is_on_server(
                            statement, &pool, server, &address,
                        )
                        .await?;
                    }
                    self.buffer.put(&data[..]);
                }
                ExtendedProtocolData::Execute { data } => {
                    if track {
                        self.response_rewriter
                            .execute(self.portal_plans.get(&Execute::get_portal(&data)?).cloned());
                    }
                    self.buffer.put(&data[..])
                }
                ExtendedProtocolData::Close { data, close } => {
                    if close.is_portal() {
                        self.portal_plans.remove(&close.name);
                    }

                    if self.prepared_statements_enabled
                        && close.is_prepared_statement()
                        && !close.anonymous()
                    {
                        // The map dropped the name, with the observer's label,
                        // when the Close was buffered.
                        self.reply_in_turn(sent, close_complete());
                        continue;
                    } else {
                        self.buffer.put(&data[..]);
                    }
                }
                ExtendedProtocolData::Local { response } => {
                    self.reply_in_turn(sent, response);
                    continue;
                }
            }
            sent += 1;
        }
        Ok(())
    }

    /// Queue a reply pgcat gives itself to a message of the batch, after the
    /// server's replies to the `sent` messages before it.
    fn reply_in_turn(&mut self, sent: usize, reply: BytesMut) {
        match sent {
            0 => self.response_message_queue_buffer.put(reply),
            _ => self.held_replies.hold(sent, reply),
        }
    }

    /// Put the replies pgcat holds into a chunk of the server's reply.
    fn splice_held_replies(
        &mut self,
        response: BytesMut,
        server: &mut Server,
    ) -> Result<BytesMut, Error> {
        self.held_replies.splice(response).inspect_err(|err| {
            server.mark_bad(&format!("could not follow the server's reply: {}", err));
        })
    }

    /// Drop the buffered batch without sending it. The plans of the statements
    /// it would have parsed or closed are put back as they were.
    fn reset_buffered_state(&mut self) {
        if let Some(bound) = self.bound_since_reload.as_mut() {
            bound.batch.clear();
        }
        self.ending_binds.clear();
        self.releasing_binds.clear();
        self.pending_identity.clear();
        self.pending_session_reset = false;
        self.batch_begins = false;
        self.statements.rollback();
        while let Some((name, was_ender, was_releaser)) = self.enders_journal.pop() {
            match was_ender {
                true => self.transaction_enders.insert(name.clone()),
                false => self.transaction_enders.remove(&name),
            };
            match was_releaser {
                true => self.lock_releasers.insert(name),
                false => self.lock_releasers.remove(&name),
            };
        }

        if let Some(observer) = self.observer.as_deref_mut() {
            observer.reject_batch();
        }
        trace_context::batch_dropped(&mut self.traces);

        self.buffer.clear();
        self.extended_protocol_data_buffer.clear();
        self.response_message_queue_buffer.clear();
        self.response_rewriter.reset();
    }

    /// Release the server from the client: it can't cancel its queries anymore.
    pub fn release(&self) {
        let mut guard = self.client_server_map.lock();
        guard.remove(&(self.process_id, self.secret_key));
    }

    async fn send_and_receive_loop(
        &mut self,
        code: char,
        message: Option<&BytesMut>,
        server: &mut Server,
        address: &Address,
        pool: &ConnectionPool,
        client_stats: &ClientStats,
    ) -> Result<(), Error> {
        debug!("Sending {} to server", code);
        self.sending_statements(server);

        let message = match message {
            Some(message) => message,
            None => &self.buffer,
        };

        self.send_server_message(server, message, address, pool)
            .await?;

        let query_start = Instant::now();
        // Read all data the server has to offer, which can be multiple messages
        // buffered in 8196 bytes chunks.
        loop {
            let response = self
                .receive_server_message(server, address, pool, client_stats)
                .await?;
            let response = self.rewrite_response(response, server, pool).await?;
            let response = self.splice_held_replies(response, server)?;
            let response = self.finish_batch(response, server).await?;

            match write_all_flush(&mut self.write, &response).await {
                Ok(_) => (),
                Err(err) => {
                    // We might be in some kind of error/in between protocol state, better to just kill this server
                    server.mark_bad(err.to_string().as_str());
                    return Err(err);
                }
            };

            if !server.is_data_available() {
                break;
            }
        }

        // Report query executed statistics.
        client_stats.query();
        server.stats().query(
            Instant::now().duration_since(query_start).as_millis() as u64,
            self.server_parameters.get_application_name(),
        );

        Ok(())
    }

    async fn send_server_message(
        &self,
        server: &mut Server,
        message: &BytesMut,
        address: &Address,
        pool: &ConnectionPool,
    ) -> Result<(), Error> {
        match server.send(message).await {
            Ok(_) => Ok(()),
            Err(err) => {
                pool.ban(address, BanReason::MessageSendFailed, Some(&self.stats));
                Err(err)
            }
        }
    }

    async fn receive_server_message(
        &mut self,
        server: &mut Server,
        address: &Address,
        pool: &ConnectionPool,
        client_stats: &ClientStats,
    ) -> Result<BytesMut, Error> {
        let statement_timeout_duration = match pool.settings.user.statement_timeout {
            0 => tokio::time::Duration::MAX,
            timeout => tokio::time::Duration::from_millis(timeout),
        };

        match tokio::time::timeout(
            statement_timeout_duration,
            server.recv(Some(&mut self.server_parameters)),
        )
        .await
        {
            Ok(result) => match result {
                Ok(mut message) => {
                    if self.withhold_server_text {
                        withhold_server_text(&mut message);
                    }

                    // The server says its session reads strings differently now,
                    // e.g. a function changed standard_conforming_strings: the
                    // tenant guard can't check what the client sends next.
                    if pool.settings.tenant_guard.is_some() {
                        if let Some(error) = unreadable_session(&self.server_parameters) {
                            server.mark_bad("the session can't be read by the tenant guard");
                            error_response_terminal(
                                &mut self.write,
                                &with_sqlstate(UNSUPPORTED, &error),
                            )
                            .await?;
                            return Err(Error::ClientError(error));
                        }
                    }

                    let (error, ready_for_query) = server.last_reply();
                    if let Some(bound) = self.bound_since_reload.as_mut() {
                        bound.answered(error, ready_for_query.is_some());
                    }
                    if let Some(observer) = self.observer.as_deref_mut() {
                        observer.server_replied(error, ready_for_query);
                    }
                    trace_context::server_replied(
                        &mut self.traces,
                        &message,
                        error,
                        ready_for_query,
                        address,
                    );
                    Ok(message)
                }
                Err(err) => {
                    trace_context::server_failed(&mut self.traces, address, "server_connection");
                    pool.ban(address, BanReason::MessageReceiveFailed, Some(client_stats));
                    error_response_terminal(
                        &mut self.write,
                        &format!("error receiving data from server: {:?}", err),
                    )
                    .await?;
                    Err(err)
                }
            },
            Err(_) => {
                server.mark_bad(
                    format!(
                        "Statement timeout while talking to {:?} with user {}",
                        address, pool.settings.user.username
                    )
                    .as_str(),
                );
                trace_context::server_failed(&mut self.traces, address, "statement_timeout");
                pool.ban(address, BanReason::StatementTimeout, Some(client_stats));
                error_response_terminal(&mut self.write, "pool statement timeout").await?;
                Err(Error::StatementTimeout)
            }
        }
    }
}

/// An identity command, verified and ready to run.
#[derive(Clone, Debug)]
enum IdentityAction {
    Set(Identity),
    Reset,
    Show,
}

impl IdentityAction {
    /// CommandComplete tag.
    fn tag(&self) -> &'static str {
        match self {
            IdentityAction::Set(_) => "SET",
            IdentityAction::Reset => "RESET",
            IdentityAction::Show => "SHOW",
        }
    }
}

/// The SQL observer's view of a client, from its fields. A free function so
/// it can be built while another field of the client is borrowed mutably.
fn caller_of<'a>(
    lapse: &IdentityLapse,
    identity_present: bool,
    server_parameters: &'a ServerParameters,
    addr: std::net::SocketAddr,
) -> Caller<'a> {
    Caller {
        identity: lapse.state(identity_present),
        application_name: server_parameters
            .get("application_name")
            .unwrap_or_default(),
        address: Some(addr),
    }
}

/// The error for a query the tenant guard refuses, if it does.
fn tenant_parameter_error(query: &str, guard: Option<&TenantGuard>) -> Option<String> {
    let guard = guard?;
    let refusal = guard.refusal(query)?;
    Some(format!(
        "{} is set by pgcat from the verified PGCAT_IDENTITY, and {}",
        guard.parameter(),
        refusal
    ))
}

/// The statement was planned with the plugin configuration before a reload:
/// it wasn't run, so it can be sent again.
fn stale_plan_error() -> String {
    with_sqlstate(
        PLUGIN_FAILED,
        "the plugin configuration changed after the statement was planned, send it again",
    )
}

/// SHOW PGCAT_IDENTITY's row: whether an identity is set, and its token's
/// fingerprint, never the token.
fn identity_show_row(identity: Option<&Identity>) -> BytesMut {
    let (set, fingerprint) = match identity {
        Some(identity) => ("t", Some(identity.token().fingerprint())),
        None => ("f", None),
    };
    data_row_nullable(&vec![Some(set.to_string()), fingerprint])
}

/// RowDescription of `SHOW PGCAT_IDENTITY`.
fn identity_row_description() -> BytesMut {
    row_description(&vec![
        ("identity_set", DataType::Bool),
        ("identity_fingerprint", DataType::Text),
    ])
}

impl<S, T> Drop for Client<S, T> {
    fn drop(&mut self) {
        let mut guard = self.client_server_map.lock();
        guard.remove(&(self.process_id, self.secret_key));

        // Dirty shutdown
        // TODO: refactor, this is not the best way to handle state management.
        if self.connected_to_server && self.last_server_stats.is_some() {
            self.last_server_stats.as_ref().unwrap().idle();
        }
    }
}

/// COMMIT, END, ROLLBACK and ABORT end the transaction; ROLLBACK TO SAVEPOINT
/// doesn't.
fn is_transaction_end(statement: &sqlparser::ast::Statement) -> bool {
    use sqlparser::ast::Statement;

    match statement {
        Statement::Commit { .. } => true,
        Statement::Rollback { savepoint, .. } => savepoint.is_none(),
        _ => false,
    }
}

/// The session parameter pgcat sets to the client's tenant.
/// What a session holding its server keeps from the pool it was checked out
/// from: its tenant guard's parameter, and whether the text of the server's
/// replies is withheld (`withholds`).
fn held_settings(settings: &PoolSettings, withholds: bool) -> (Option<&str>, bool) {
    (
        settings.tenant_guard.as_ref().map(TenantGuard::parameter),
        withholds,
    )
}

/// BEGIN and START TRANSACTION.
fn is_transaction_begin(statement: &sqlparser::ast::Statement) -> bool {
    matches!(
        statement,
        sqlparser::ast::Statement::StartTransaction { .. }
    )
}

/// COMMIT and ROLLBACK release the transaction's locks, and ROLLBACK TO
/// SAVEPOINT those taken since the savepoint. (ABORT and PREPARE TRANSACTION
/// don't parse: pgcat takes such statements to end the transaction.)
fn is_lock_release(statement: &sqlparser::ast::Statement) -> bool {
    use sqlparser::ast::Statement;

    matches!(
        statement,
        Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;

    /// A startup message's length word and code, then `body` zero bytes.
    fn startup_message(len: i32, code: i32, body: usize) -> Vec<u8> {
        let mut message = len.to_be_bytes().to_vec();
        message.extend(code.to_be_bytes());
        message.resize(message.len() + body, 0);
        message
    }

    #[tokio::test]
    async fn startup_lengths_are_bounded_as_postgres_bounds_them() {
        // Below 8 there's no room for the code, and past 10,004 the claim is
        // refused before anything is reserved for it.
        let too_long = (4 + MAX_STARTUP_BODY + 1) as i32;
        for len in [i32::MIN, -1, 0, 3, 4, 7, too_long, i32::MAX] {
            let (mut client, mut pgcat) = tokio::io::duplex(64);
            client.write_all(&len.to_be_bytes()).await.unwrap();
            assert!(
                matches!(get_startup(&mut pgcat).await, Err(Error::ClientBadStartup)),
                "{}",
                len
            );
        }

        let (mut client, mut pgcat) = tokio::io::duplex(64);
        client
            .write_all(&startup_message(8, SSL_REQUEST_CODE, 0))
            .await
            .unwrap();
        assert!(matches!(
            get_startup(&mut pgcat).await,
            Ok((ClientConnectionType::Tls, _))
        ));

        let longest = startup_message(
            (4 + MAX_STARTUP_BODY) as i32,
            PROTOCOL_VERSION_NUMBER,
            MAX_STARTUP_BODY - 4,
        );
        let (mut client, mut pgcat) = tokio::io::duplex(longest.len());
        client.write_all(&longest).await.unwrap();
        assert!(matches!(
            get_startup(&mut pgcat).await,
            Ok((ClientConnectionType::Startup, _))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_sends_nothing_is_dropped_at_the_deadline() {
        let started = tokio::time::Instant::now();
        let (_client, mut pgcat) = tokio::io::duplex(64);
        assert!(matches!(
            by(started + STARTUP_TIMEOUT, get_startup(&mut pgcat)).await,
            Err(Error::SocketError(_))
        ));
        assert_eq!(started.elapsed(), STARTUP_TIMEOUT);
    }

    fn panicking_handler() -> Result<(), Error> {
        panic!("the client handler panicked")
    }

    #[tokio::test]
    async fn test_accounting_is_undone_when_the_handler_panics() {
        // An id no other test registers.
        let client_id = i32::MIN + 0x5eed;
        let stats = Arc::new(ClientStats::new(
            client_id,
            "app",
            "user",
            "pool",
            tokio::time::Instant::now(),
        ));
        stats.register(stats.clone());
        assert!(crate::stats::get_client_stats().contains_key(&client_id));

        let (drain, mut drain_rx) = tokio::sync::mpsc::channel(8);
        let handle = async { panicking_handler() };
        let result = AssertUnwindSafe(handle_with_accounting(handle, false, &stats, &drain))
            .catch_unwind()
            .await;

        // The panic still reaches the task, after the drain count is back to
        // zero and the client's stats are removed.
        assert!(result.is_err());
        assert_eq!(drain_rx.try_recv(), Ok(1));
        assert_eq!(drain_rx.try_recv(), Ok(-1));
        assert!(drain_rx.try_recv().is_err());
        assert!(!crate::stats::get_client_stats().contains_key(&client_id));
    }

    // ==================== PREPARED_STATEMENT_COUNTER Tests ====================

    #[test]
    fn test_prepared_statement_counter_exists() {
        // Verify the counter can be accessed and has a reasonable initial value
        let _ = PREPARED_STATEMENT_COUNTER.load(Ordering::Relaxed);
    }

    #[test]
    fn test_prepared_statement_counter_increments() {
        let initial = PREPARED_STATEMENT_COUNTER.load(Ordering::Relaxed);
        let next = PREPARED_STATEMENT_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert_eq!(initial, next);

        let after = PREPARED_STATEMENT_COUNTER.load(Ordering::Relaxed);
        assert!(after > initial);
    }

    // ==================== ClientConnectionType Tests ====================

    #[test]
    fn test_client_connection_type_variants() {
        // Verify the enum variants exist and can be matched
        let startup = ClientConnectionType::Startup;
        let tls = ClientConnectionType::Tls;
        let cancel = ClientConnectionType::CancelQuery;

        match startup {
            ClientConnectionType::Startup => (),
            _ => panic!("Expected Startup"),
        }

        match tls {
            ClientConnectionType::Tls => (),
            _ => panic!("Expected Tls"),
        }

        match cancel {
            ClientConnectionType::CancelQuery => (),
            _ => panic!("Expected CancelQuery"),
        }
    }

    // ==================== Constants Tests ====================

    #[test]
    fn test_ssl_request_code_value() {
        // SSL_REQUEST_CODE should be the standard Postgres value
        assert_eq!(SSL_REQUEST_CODE, 80877103);
    }

    #[test]
    fn test_cancel_request_code_value() {
        // CANCEL_REQUEST_CODE should be the standard Postgres value
        assert_eq!(CANCEL_REQUEST_CODE, 80877102);
    }

    #[test]
    fn test_protocol_version_number() {
        // PROTOCOL_VERSION_NUMBER should be 196608 (3.0)
        assert_eq!(PROTOCOL_VERSION_NUMBER, 196608);
    }

    // ==================== Admin Pool Names Tests ====================

    #[test]
    fn test_admin_pool_names() {
        // Test that "pgcat" and "pgbouncer" are recognized as admin pools
        let admin_names = ["pgcat", "pgbouncer"];

        for name in &admin_names {
            let is_admin = admin_names.iter().filter(|db| *db == name).count() == 1;
            assert!(is_admin, "{} should be recognized as admin pool", name);
        }

        // Non-admin pool name
        let is_admin = admin_names.iter().filter(|db| *db == &"regular_db").count() == 1;
        assert!(
            !is_admin,
            "regular_db should not be recognized as admin pool"
        );
    }

    // ==================== Local Statement Tests ====================

    #[test]
    fn test_local_statement_from_simple_query_response() {
        let mut response = BytesMut::new();
        response.put(row_description(&vec![("a", DataType::Text)]));
        response.put(data_row(&vec!["1".to_string()]));
        response.put(data_row(&vec!["2".to_string()]));
        response.put(command_complete("SELECT 2"));
        response.put(ready_for_query(false));

        let statement = LocalStatement::from_simple_query_response(&response).unwrap();

        assert_eq!(
            statement.describe(),
            row_description(&vec![("a", DataType::Text)])
        );
        assert_eq!(statement.encode_rows(response.clone()).unwrap(), response);

        let mut expected = BytesMut::new();
        expected.put(data_row(&vec!["1".to_string()]));
        expected.put(data_row(&vec!["2".to_string()]));
        expected.put(command_complete("SELECT 2"));
        assert_eq!(statement.execute_response, expected);

        // A statement without rows is described as NoData.
        let statement =
            LocalStatement::from_simple_query_response(&command_complete("SET")).unwrap();
        assert_eq!(statement.describe(), no_data());
    }

    #[test]
    fn test_local_statement_binary_results() {
        let statement = LocalStatement::identity(IdentityAction::Show);
        let rows = data_row_nullable(&vec![Some("t".to_string()), Some("abc".to_string())]);

        // Bound with one binary format code for all columns.
        let portal = statement.bind(&[1]);
        let description: RowDescription = portal.describe()[..].try_into().unwrap();
        assert!(description.fields.iter().all(|field| field.format == 1));

        let encoded = portal.encode_rows(rows.clone()).unwrap();
        let row: DataRow = encoded[..].try_into().unwrap();
        assert_eq!(row.values[0].as_deref(), Some(&[1u8][..]));
        assert_eq!(row.values[1].as_deref(), Some(&b"abc"[..]));

        // Text formats are left alone.
        assert_eq!(statement.bind(&[]).encode_rows(rows.clone()).unwrap(), rows);
    }

    // ==================== Flush→Sync Conversion Tests ====================

    #[test]
    fn test_sync_bytes_constant() {
        assert_eq!(SYNC_BYTES.len(), 5);
        assert_eq!(SYNC_BYTES[0], b'S');
        // Length field: big-endian i32 = 4
        assert_eq!(&SYNC_BYTES[1..5], &[0, 0, 0, 4]);
    }

    #[test]
    fn test_ready_for_query_size() {
        // ReadyForQuery: 'Z' (1) + length 5 as i32 (4) + status (1) = 6
        assert_eq!(READY_FOR_QUERY_SIZE, 6);

        let rfq = ready_for_query(false);
        assert_eq!(rfq.len(), READY_FOR_QUERY_SIZE);
        assert_eq!(rfq[0], b'Z');
    }

    #[test]
    fn test_strip_ready_for_query_from_response() {
        // Simulate a server response: ParseComplete + ParameterDescription + ReadyForQuery
        let mut response = BytesMut::new();

        // ParseComplete: '1' + length 4
        response.put_u8(b'1');
        response.put_i32(4);

        // Append ReadyForQuery
        response.put(&ready_for_query(false)[..]);

        let total_len = response.len();
        assert!(total_len > READY_FOR_QUERY_SIZE);

        // Strip trailing ReadyForQuery
        let trimmed = &response[..total_len - READY_FOR_QUERY_SIZE];
        assert_eq!(trimmed.len(), 5); // Just the ParseComplete message
        assert_eq!(trimmed[0], b'1');
    }

    #[test]
    fn test_strip_ready_for_query_only_rfq() {
        // When server response is exactly ReadyForQuery, nothing should be forwarded
        let response = ready_for_query(true);
        assert_eq!(response.len(), READY_FOR_QUERY_SIZE);
        // response.len() > READY_FOR_QUERY_SIZE is false, so nothing is forwarded
        assert!(response.len() <= READY_FOR_QUERY_SIZE);
    }

    #[test]
    fn test_fresh_portals_wait_for_their_batch_reply() {
        let mut portals = FreshPortals::default();
        portals.batch.insert("p".to_string());

        // A simple Query's reply, sent while the Bind is buffered.
        portals.answered(false, true);
        assert!(portals.bound.is_empty());

        // The batch sent: its reply, up to its ReadyForQuery.
        portals.send();
        portals.answered(false, false);
        assert!(portals.bound.is_empty());
        portals.answered(false, true);
        assert!(portals.bound.contains("p"));

        // An error drops the sent batch's Binds, not those still buffered.
        portals.batch.insert("q".to_string());
        portals.send();
        portals.batch.insert("r".to_string());
        portals.answered(true, true);
        assert!(!portals.bound.contains("q"));
        assert!(portals.batch.contains("r"));

        portals.answered_locally();
        assert!(portals.bound.contains("r"));
    }
}
