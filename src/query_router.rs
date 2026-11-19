/// Route queries automatically based on explicitly requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use log::{debug, error};
use mini_moka::sync::Cache;
use once_cell::sync::{Lazy, OnceCell};
use regex::{Regex, RegexSet};
use sqlparser::ast::Statement::{Delete, Insert, Query, StartTransaction, Update};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, Ident, JoinConstraint, JoinOperator, ObjectName, SetExpr,
    Statement, TableFactor, TableObject, TableWithJoins, UpdateTableFromKind, Value, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::sync::Arc;
use std::sync::OnceLock;

use crate::config::Role;
use crate::errors::Error;
use crate::identity::{Identity, IdentityToken, VerifiedIdentity};
use crate::messages::BytesMutReader;
use crate::plugins::hook_errors::{self, Hook, NO_PLUGIN};
use crate::plugins::registry::{self, PluginRegistry, Section};
use crate::plugins::{
    Intercept, Plugin, PluginContext, PluginOutput, PluginResult, QueryLogger, TableAccess,
};
use crate::pool::PoolSettings;
use crate::sharding::Sharder;

use std::collections::BTreeSet;
use std::io::Cursor;
use std::time::Duration;
use std::{cmp, mem};

/// Regexes used to parse custom commands.
const CUSTOM_SQL_REGEXES: [&str; 7] = [
    r"(?i)^ *SET SHARDING KEY TO '?([0-9]+)'? *;? *$",
    r"(?i)^ *SET SHARD TO '?([0-9]+|ANY)'? *;? *$",
    r"(?i)^ *SHOW SHARD *;? *$",
    r"(?i)^ *SET SERVER ROLE TO '(PRIMARY|REPLICA|ANY|AUTO|DEFAULT)' *;? *$",
    r"(?i)^ *SHOW SERVER ROLE *;? *$",
    r"(?i)^ *SET PRIMARY READS TO '?(on|off|default)'? *;? *$",
    r"(?i)^ *SHOW PRIMARY READS *;? *$",
];

/// Custom commands.
#[derive(PartialEq, Debug)]
pub enum Command {
    SetShardingKey,
    SetShard,
    ShowShard,
    SetServerRole,
    ShowServerRole,
    SetPrimaryReads,
    ShowPrimaryReads,
}

#[derive(PartialEq, Debug)]
pub enum ShardingKey {
    Value(i64),
    Placeholder(i16),
}

#[derive(Clone, Debug)]
enum ParameterFormat {
    Text,
    Binary,
    Uniform(Box<ParameterFormat>),
    Specified(Vec<ParameterFormat>),
}

/// Statements the parser doesn't understand that can't reference a table.
static TABLE_FREE_UNPARSED_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)^\s*RESET\s+[a-z_][a-z0-9_.]*\s*;?\s*$").unwrap());

/// The first word of a query, after the whitespace, comments and empty
/// statements before it.
fn first_word(query: &str) -> &str {
    let mut rest = query;
    loop {
        // Postgres drops empty statements, so the first word can follow `;`.
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == ';');
        if let Some(comment) = rest.strip_prefix("--") {
            rest = comment.find(['\n', '\r']).map_or("", |end| &comment[end..]);
        } else if rest.starts_with("/*") {
            // Block comments nest.
            let bytes = rest.as_bytes();
            let (mut depth, mut i) = (1, 2);
            while depth > 0 {
                match bytes.get(i..i + 2) {
                    Some(b"/*") => (depth, i) = (depth + 1, i + 2),
                    Some(b"*/") => (depth, i) = (depth - 1, i + 2),
                    Some(_) => i += 1,
                    None => return "",
                }
            }
            rest = &rest[i..];
        } else {
            // Postgres reads letters, digits, `_`, `$` and non-ASCII
            // characters as part of a word.
            let end = rest
                .find(|c: char| {
                    c.is_ascii() && !(c.is_ascii_alphanumeric() || c == '_' || c == '$')
                })
                .unwrap_or(rest.len());
            return &rest[..end];
        }
    }
}

/// How many times [`runs_string_body`] tokenizes `COPY ... FROM STDIN` data
/// again before it refuses the query.
const COPY_DATA_READS: usize = 4;

/// Whether a query has a statement that runs a string as code: `DO`, or
/// `CREATE [OR REPLACE] FUNCTION` or `PROCEDURE`, whose bodies are strings
/// the parser doesn't read. One that has one of those words and can't be
/// tokenized might.
fn runs_string_body(query: &str) -> bool {
    // Keywords can't be escaped, so a statement can only start with these if
    // the text has them. Queries without any of the three substrings skip
    // tokenizing.
    let has = |word: &[u8]| {
        query
            .as_bytes()
            .windows(word.len())
            .any(|window| window.eq_ignore_ascii_case(word))
    };
    if !has(b"do") && !has(b"function") && !has(b"procedure") {
        return false;
    }
    let tokenize = |text: &str| Tokenizer::new(&PostgreSqlDialect {}, text).tokenize();
    let Ok(mut tokens) = tokenize(query) else {
        return true;
    };
    // The tokenizer reads the lines after `COPY ... FROM STDIN;` as the
    // COPY's data, up to a `\.` line, and puts them in one token before the
    // `;`. The server reads them as statements, since COPY data comes in
    // messages of its own. Data that runs to the end of the query is
    // tokenized again and put back after the rest of the `;` line, where it
    // is in the text. Data ended by a `\.` line is refused, since the server
    // can read that line as part of a comment or string, and so is a query
    // where this happens more than `COPY_DATA_READS` times, which keeps the
    // cost linear.
    let mut reads = 0;
    while let Some(index) = tokens
        .iter()
        .position(|token| matches!(token, Token::CopyData { .. }))
    {
        reads += 1;
        if reads > COPY_DATA_READS {
            return true;
        }
        let Token::CopyData {
            data,
            terminated: false,
        } = tokens.remove(index)
        else {
            return true;
        };
        let Ok(more) = tokenize(&data) else {
            return true;
        };
        tokens.extend(more);
    }
    tokens
        .split(|token| *token == Token::SemiColon)
        .any(|statement| {
            let mut keywords = statement
                .iter()
                .filter(|token| !matches!(token, Token::Whitespace(_)))
                .map(|token| match token {
                    // Quoted words have no keyword.
                    Token::Word(word) => word.keyword,
                    _ => Keyword::NoKeyword,
                });
            let first: [Keyword; 4] =
                std::array::from_fn(|_| keywords.next().unwrap_or(Keyword::NoKeyword));
            matches!(
                first,
                [Keyword::DO, ..]
                    | [Keyword::CREATE, Keyword::FUNCTION | Keyword::PROCEDURE, ..]
                    | [
                        Keyword::CREATE,
                        Keyword::OR,
                        Keyword::REPLACE,
                        Keyword::FUNCTION | Keyword::PROCEDURE
                    ]
            )
        })
}

/// Quickly test for match when a query is received.
static CUSTOM_SQL_REGEX_SET: OnceCell<RegexSet> = OnceCell::new();

// Get the value inside the custom command.
static CUSTOM_SQL_REGEX_LIST: OnceCell<Vec<Regex>> = OnceCell::new();

#[derive(Debug, Clone, PartialEq)]
enum DatabaseActivityState {
    Active,
    Initializing,
}

// A moka cache for the databases
// the key is the database name and the value is the database activity state
static DATABASE_ACTIVITY_CACHE: OnceLock<Cache<String, DatabaseActivityState>> = OnceLock::new();
// A moka cache for the tables, the key is the db_table.
static TABLE_MUTATIONS_CACHE: OnceLock<Cache<String, bool>> = OnceLock::new();

/// Refuses identities on a pool where more than one plugin verifies them,
/// which config validation doesn't allow.
const MANY_VERIFIERS: &str = "more than one plugin verifies identities on the pool";

/// What the client gets instead of a configured verifier's refusal that
/// might quote the token.
const REDACTED_REFUSAL: &str = "the plugin refused the identity";

/// Whether a refusal has 20 or more characters in a row a token is made of
/// (`A-Z a-z 0-9 - _`): the token, without its `v1.`, or a slice of it. One
/// pass, so the cost depends on the message's length only; a long identifier
/// is hidden too. Considered instead: always sending [`REDACTED_REFUSAL`],
/// which loses the plugin's reason, and searching for the token and the token
/// without `v1.`, which misses a cut or sliced one.
fn might_quote_token(message: &str) -> bool {
    let mut run = 0;
    message.bytes().any(|byte| {
        let in_token = byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_';
        run = if in_token { run + 1 } else { 0 };
        run >= 20
    })
}

/// Why a token is verified, which decides the SQLSTATE a client gets when
/// it isn't.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Verifying {
    /// `SET PGCAT_IDENTITY`: a refusal is 28000.
    Set,
    /// The identity a client holds, again after a reload changed its
    /// verifier: a refusal ends the session with 0A000 (`Identity::recheck`).
    Again,
}

/// `plugin`'s answer to a token, its refusal or failure counted with the
/// SQLSTATE the client gets: the hook's (PH001, PH002), else 28000 for a
/// `SET`, or 0A000 when verified `Again`.
fn counted(
    plugin: &'static str,
    verifying: Verifying,
    verified: Result<VerifiedIdentity, String>,
) -> Result<VerifiedIdentity, String> {
    if let Err(error) = &verified {
        let code = match (crate::messages::sqlstate_of(error).0, verifying) {
            (Some(code @ (crate::plugins::HOOK_TIMED_OUT | crate::plugins::HOOK_BUSY)), _) => code,
            (_, Verifying::Again) => crate::plugins::UNSUPPORTED,
            (code, Verifying::Set) => code.unwrap_or(crate::plugins::IDENTITY_REFUSED),
        };
        hook_errors::count_code(plugin, Hook::VerifyIdentity, Some(code));
    }
    verified
}

/// The query router.
pub struct QueryRouter {
    /// Which shard we should be talking to right now.
    active_shard: Option<usize>,

    /// Which server should we be talking to.
    active_role: Option<Role>,

    /// Should we try to parse queries to route them to replicas or primary automatically
    query_parser_enabled: Option<bool>,

    /// Include the primary into the replica pool for reads.
    primary_reads_enabled: Option<bool>,

    /// Pool configuration.
    pool_settings: PoolSettings,

    // Placeholders from prepared statement.
    placeholders: Vec<i16>,
    registered_plugins: Arc<PluginRegistry>,

    /// `plan_generation()`, kept current with the pool settings.
    plan_generation: u64,

    /// The plugin the plugin chain last called: the one a chain error or
    /// timeout is counted for.
    running: parking_lot::Mutex<&'static str>,
}

struct ExtractedExprsAndTables<'a> {
    exprs: Vec<Expr>,
    table_names: Vec<Vec<Ident>>,
    assignments_opt: Option<&'a Vec<Assignment>>,
}

/// A statement names a table `ONLY`: the parser's reading of `ONLY t` where it
/// doesn't take ONLY as the inheritance marker, as in `INSERT INTO ONLY t`. An
/// unquoted ONLY is a reserved word, never a table's name.
fn names_only(statement: &Statement) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_relations(statement, |relation| {
        found |= matches!(relation.0.as_slice(),
        [name] if name.as_ident().is_some_and(|name| {
            name.quote_style.is_none() && name.value.eq_ignore_ascii_case("only")
        }));
        std::ops::ControlFlow::<()>::Continue(())
    });
    found
}

/// A statement has data for `COPY ... FROM STDIN` in the query text. The parser
/// takes the text from the line after the COPY as that data, but the server
/// reads it as more statements: COPY data comes in CopyData messages. Blank
/// text can't be a statement.
fn copies_inline_data(statement: &Statement) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_statements(statement, |statement| {
        found |= matches!(statement, Statement::Copy { payload: Some(payload), .. }
            if !payload.data.bytes().all(|byte| byte.is_ascii_whitespace()));
        std::ops::ControlFlow::<()>::Continue(())
    });
    found
}

impl QueryRouter {
    /// One-time initialization of regexes
    /// that parse our custom SQL protocol.
    pub fn setup() -> bool {
        let set = match RegexSet::new(CUSTOM_SQL_REGEXES) {
            Ok(rgx) => rgx,
            Err(err) => {
                error!("QueryRouter::setup Could not compile regex set: {:?}", err);
                return false;
            }
        };

        let list: Vec<_> = CUSTOM_SQL_REGEXES
            .iter()
            .map(|rgx| Regex::new(rgx).unwrap())
            .collect();

        assert_eq!(list.len(), set.len());

        match CUSTOM_SQL_REGEX_LIST.set(list) {
            Ok(_) => true,
            Err(_) => return false,
        };

        CUSTOM_SQL_REGEX_SET.set(set).is_ok()
    }

    /// Create a new instance of the query router.
    /// Each client gets its own.
    pub fn new() -> QueryRouter {
        Self::with_plugins(registry::process_plugins())
    }

    /// Explicit injection keeps library callers and tests independent of the
    /// process-wide runner. The registry is shared, never copied per statement.
    pub fn with_plugins(registered_plugins: Arc<PluginRegistry>) -> QueryRouter {
        let mut router = QueryRouter {
            active_shard: None,
            active_role: None,
            query_parser_enabled: None,
            primary_reads_enabled: None,
            pool_settings: PoolSettings::default(),
            placeholders: Vec::new(),
            registered_plugins,
            plan_generation: 0,
            running: parking_lot::Mutex::new(NO_PLUGIN),
        };
        router.plan_generation = router.compute_plan_generation();
        router
    }

    /// Pool settings can change because of a config reload.
    pub fn update_pool_settings(&mut self, pool_settings: &PoolSettings) {
        self.pool_settings = pool_settings.clone();
        self.plan_generation = self.compute_plan_generation();
    }

    pub fn pool_settings(&self) -> &PoolSettings {
        &self.pool_settings
    }

    /// Try to parse a command and execute it.
    pub fn try_execute_command(&mut self, message_buffer: &BytesMut) -> Option<(Command, String)> {
        let mut message_cursor = Cursor::new(message_buffer);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32() as usize;

        let comment_shard_routing_enabled = self.pool_settings.shard_id_regex.is_some()
            || self.pool_settings.sharding_key_regex.is_some();

        // Check for any sharding regex matches in any queries
        if comment_shard_routing_enabled {
            match code {
                // For Parse and Query messages peek to see if they specify a shard_id as a comment early in the statement
                'P' | 'Q' => {
                    // Check only the first block of bytes configured by the pool settings
                    let seg = cmp::min(len - 5, self.pool_settings.regex_search_limit);

                    let query_start_index = mem::size_of::<u8>() + mem::size_of::<i32>();

                    let initial_segment = String::from_utf8_lossy(
                        &message_buffer[query_start_index..query_start_index + seg],
                    );

                    // Check for a shard_id included in the query
                    if let Some(shard_id_regex) = &self.pool_settings.shard_id_regex {
                        let shard_id = shard_id_regex.captures(&initial_segment).and_then(|cap| {
                            cap.get(1).and_then(|id| id.as_str().parse::<usize>().ok())
                        });
                        if let Some(shard_id) = shard_id {
                            debug!("Setting shard to {:?}", shard_id);
                            self.set_shard(Some(shard_id));
                            // Skip other command processing since a sharding command was found
                            return None;
                        }
                    }

                    // Check for a sharding_key included in the query
                    if let Some(sharding_key_regex) = &self.pool_settings.sharding_key_regex {
                        let sharding_key =
                            sharding_key_regex
                                .captures(&initial_segment)
                                .and_then(|cap| {
                                    cap.get(1).and_then(|id| id.as_str().parse::<i64>().ok())
                                });
                        if let Some(sharding_key) = sharding_key {
                            // The key is a value from the query's text.
                            if !self.redact_queries() {
                                debug!("Setting sharding_key to {:?}", sharding_key);
                            }
                            self.set_sharding_key(sharding_key);
                            // Skip other command processing since a sharding command was found
                            return None;
                        }
                    }
                }
                _ => {}
            }
        }

        // Only simple protocol supported for commands processed below
        if code != 'Q' {
            return None;
        }

        let query = message_cursor.read_string().unwrap();

        let regex_set = match CUSTOM_SQL_REGEX_SET.get() {
            Some(regex_set) => regex_set,
            None => return None,
        };

        let regex_list = match CUSTOM_SQL_REGEX_LIST.get() {
            Some(regex_list) => regex_list,
            None => return None,
        };

        let matches: Vec<_> = regex_set.matches(&query).into_iter().collect();

        // This is not a custom query, try to infer which
        // server it'll go to if the query parser is enabled.
        if matches.len() != 1 {
            debug!("Regular query, not a command");
            return None;
        }

        let command = match matches[0] {
            0 => Command::SetShardingKey,
            1 => Command::SetShard,
            2 => Command::ShowShard,
            3 => Command::SetServerRole,
            4 => Command::ShowServerRole,
            5 => Command::SetPrimaryReads,
            6 => Command::ShowPrimaryReads,
            _ => unreachable!(),
        };

        let mut value = match command {
            Command::SetShardingKey
            | Command::SetShard
            | Command::SetServerRole
            | Command::SetPrimaryReads => {
                // Capture value. I know this re-runs the regex engine, but I haven't
                // figured out a better way just yet. I think I can write a single Regex
                // that matches all 5 custom SQL patterns, but maybe that's not very legible?
                //
                // I think this is faster than running the Regex engine 5 times.
                match regex_list[matches[0]].captures(&query) {
                    Some(captures) => match captures.get(1) {
                        Some(value) => value.as_str().to_string(),
                        None => return None,
                    },
                    None => return None,
                }
            }

            Command::ShowShard => self
                .shard()
                .map_or_else(|| "unset".to_string(), |x| x.to_string()),
            Command::ShowServerRole => match self.active_role {
                Some(Role::Primary) => Role::Primary.to_string(),
                Some(Role::Replica) => Role::Replica.to_string(),
                Some(Role::Mirror) => Role::Mirror.to_string(),
                None => {
                    if self.query_parser_enabled() {
                        String::from("auto")
                    } else {
                        String::from("any")
                    }
                }
            },

            Command::ShowPrimaryReads => match self.primary_reads_enabled() {
                true => String::from("on"),
                false => String::from("off"),
            },
        };

        match command {
            Command::SetShardingKey => {
                // TODO: some error handling here
                value = self
                    .set_sharding_key(value.parse::<i64>().unwrap())
                    .unwrap()
                    .to_string();
            }

            Command::SetShard => {
                self.active_shard = match value.to_ascii_uppercase().as_ref() {
                    "ANY" => Some(rand::random_range(0..self.pool_settings.shards)),
                    _ => Some(value.parse::<usize>().unwrap()),
                };
            }

            Command::SetServerRole => {
                self.active_role = match value.to_ascii_lowercase().as_ref() {
                    "primary" => {
                        self.query_parser_enabled = Some(false);
                        Some(Role::Primary)
                    }

                    "replica" => {
                        self.query_parser_enabled = Some(false);
                        Some(Role::Replica)
                    }

                    "any" => {
                        self.query_parser_enabled = Some(false);
                        None
                    }

                    "auto" => {
                        self.query_parser_enabled = Some(true);
                        None
                    }

                    "default" => {
                        self.active_role = self.pool_settings.default_role;
                        self.query_parser_enabled = None;
                        self.active_role
                    }

                    _ => unreachable!(),
                };
            }

            Command::SetPrimaryReads => {
                if value == "on" {
                    debug!("Setting primary reads to on");
                    self.primary_reads_enabled = Some(true);
                } else if value == "off" {
                    debug!("Setting primary reads to off");
                    self.primary_reads_enabled = Some(false);
                } else if value == "default" {
                    debug!("Setting primary reads to default");
                    self.primary_reads_enabled = None;
                }
            }

            _ => (),
        }

        Some((command, value))
    }

    pub fn parse(&self, message: &BytesMut) -> Result<Vec<Statement>, Error> {
        let mut message_cursor = Cursor::new(message);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32() as usize;

        if let Some(max_length) = self.pool_settings.query_parser_max_length {
            if len > max_length {
                return Err(Error::QueryRouterParserError(format!(
                    "Query too long for parser: {} > {}",
                    len, max_length
                )));
            }
        };

        let query = match code {
            // Query
            'Q' => {
                let query = message_cursor.read_string()?;
                debug!("Parsing simple query");
                query
            }

            // Parse (prepared statement)
            'P' => {
                // Reads statement name
                let _name = message_cursor.read_string()?;

                // Reads query string
                let query = message_cursor.read_string()?;

                debug!("Parsing prepared statement");

                query
            }

            _ => return Err(Error::UnsupportedStatement),
        };

        Self::parse_sql(&query)
    }

    /// Parse a plugin's rewrite again from the SQL that's sent, with the limits
    /// of a client's query, so it's checked as the server will read it.
    fn parse_rewrite(&self, sql: &str) -> Result<Vec<Statement>, Error> {
        match self.pool_settings.query_parser_max_length {
            Some(max_length) if sql.len() > max_length => Err(Error::QueryRouterParserError(
                "rewrite too long for parser".into(),
            )),
            _ => Self::parse_sql(sql),
        }
    }

    fn parse_sql(query: &str) -> Result<Vec<Statement>, Error> {
        match Parser::parse_sql(&PostgreSqlDialect {}, query) {
            // `FROM ONLY t` and `UPDATE ONLY t` name t, so plugins check t, but
            // the parser reads `INSERT INTO ONLY t` as a table named ONLY,
            // aliased t: what plugins check wouldn't be the table the server reads.
            Ok(ast) if ast.iter().any(names_only) => Err(Error::QueryRouterParserError(
                "ONLY before a table name isn't supported".into(),
            )),
            // Statements the parser took as COPY data: plugins wouldn't see them.
            Ok(ast) if ast.iter().any(copies_inline_data) => Err(Error::QueryRouterParserError(
                "text after COPY FROM STDIN on a new line isn't supported".into(),
            )),
            Ok(ast) => Ok(ast),
            Err(_) => {
                // Parser errors can quote SQL literals, including passwords.
                // Callers log this error and may also return it to the client.
                debug!("SQL parsing failed");
                Err(Error::QueryRouterParserError(
                    "SQL syntax is not supported by the query parser".into(),
                ))
            }
        }
    }

    /// Determines if a query is a mutation or not: its body or a query in its
    /// `WITH` clause writes.
    fn is_mutation_query(q: &sqlparser::ast::Query) -> bool {
        let cte_mutates = q.with.as_ref().is_some_and(|with| {
            with.cte_tables
                .iter()
                .any(|cte| Self::is_mutation_query(&cte.query))
        });
        cte_mutates || Self::is_mutation_set_expr(&q.body)
    }

    /// Determines if a query body writes. The match lists every kind of body,
    /// so a new one has to be classified here.
    fn is_mutation_set_expr(body: &SetExpr) -> bool {
        match body {
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
                true
            }
            SetExpr::Query(q) => Self::is_mutation_query(q),
            SetExpr::SetOperation { left, right, .. } => {
                Self::is_mutation_set_expr(left) || Self::is_mutation_set_expr(right)
            }
            SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) => false,
        }
    }

    fn database_activity_cache(&self) -> Cache<String, DatabaseActivityState> {
        DATABASE_ACTIVITY_CACHE
            .get_or_init(|| {
                Cache::builder()
                    .time_to_idle(Duration::from_secs(self.pool_settings.db_activity_ttl))
                    .build()
            })
            .clone()
    }

    /// Check database activity state and reset it if necessary
    fn database_activity_state(&self, db: &String) -> DatabaseActivityState {
        let cache = self.database_activity_cache();

        // Exists in cache
        if cache.contains_key(db) {
            return cache.get(db).unwrap();
        }

        // Not in cache
        debug!("Adding database to cache: {}", db);

        cache.insert(db.to_string(), DatabaseActivityState::Initializing);

        // Set a timer to update the cache
        let db = db.clone();
        let db_activity_init_delay = self.pool_settings.db_activity_init_delay;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(db_activity_init_delay)).await;
            cache.insert(db, DatabaseActivityState::Active);
        });

        DatabaseActivityState::Initializing
    }

    /// Try to infer which server to connect to based on the contents of the query.
    pub fn infer(&mut self, ast: &Vec<sqlparser::ast::Statement>) -> Result<(), Error> {
        if !self.pool_settings.query_parser_read_write_splitting {
            return Ok(()); // Nothing to do
        }

        debug!("Inferring role");

        if ast.is_empty() {
            // That's weird, no idea, let's go to primary
            self.active_role = Some(Role::Primary);
            return Err(Error::QueryRouterParserError("empty query".into()));
        }

        let mut primary_set_based_on_activity = false;
        let mut visited_write_statement = false;
        let mut prev_inferred_shard = None;

        if self.pool_settings.db_activity_based_routing {
            let db = self.pool_settings.db.clone();
            let state = self.database_activity_state(&db);
            debug!("Database activity state: {:?}", state);

            if let DatabaseActivityState::Initializing = state {
                debug!("Database is initializing, going to primary");

                self.active_role = Some(Role::Primary);
                primary_set_based_on_activity = true;
            }
        }

        for q in ast {
            match q {
                // All transactions go to the primary, probably a write.
                StartTransaction { .. } => {
                    self.active_role = Some(Role::Primary);
                    break;
                }

                // Likely a read-only query
                Query(query) => {
                    if primary_set_based_on_activity {
                        // If we already set the role based on activity, we don't need to do it again
                        continue;
                    }

                    if self.pool_settings.db_activity_based_routing {
                        // Check if the tables in the query have been written to recently
                        if self.query_handles_tables_in_mutation_cache(query) {
                            debug!("Query handles tables in mutation cache, going to primary");

                            self.active_role = Some(Role::Primary);
                            primary_set_based_on_activity = true;
                            continue;
                        }
                    }

                    match &self.pool_settings.automatic_sharding_key {
                        Some(_) => {
                            // TODO: if we have multiple queries in the same message,
                            // we can either split them and execute them individually
                            // or discard shard selection. If they point to the same shard though,
                            // we can let them through as-is.
                            // This is basically building a database now :)
                            let inferred_shard = self.infer_shard(query);
                            self.handle_inferred_shard(inferred_shard, &mut prev_inferred_shard)?;
                        }

                        None => (),
                    };

                    let has_locks = !query.locks.is_empty();
                    let has_mutation = Self::is_mutation_query(query);

                    if has_locks || has_mutation {
                        visited_write_statement = true;
                        self.active_role = Some(Role::Primary);
                    } else if !visited_write_statement {
                        // If we already visited a write statement, we should be going to the primary.
                        self.active_role = match self.primary_reads_enabled() {
                            false => Some(Role::Replica), // If primary should not be receiving reads, use a replica.
                            true => None,                 // Any server role is fine in this case.
                        }
                    }
                }

                // Likely a write
                _ => {
                    debug!("Write statement found, going to primary");

                    if self.pool_settings.db_activity_based_routing {
                        // add all of the query tables to the mutation cache
                        self.update_mutation_cache_on_write(q);
                    }

                    match &self.pool_settings.automatic_sharding_key {
                        Some(_) => {
                            // TODO: similar to the above, if we have multiple queries in the
                            // same message, we can either split them and execute them individually
                            // or discard shard selection. If they point to the same shard though,
                            // we can let them through as-is.
                            let inferred_shard = self.infer_shard_on_write(q)?;
                            self.handle_inferred_shard(inferred_shard, &mut prev_inferred_shard)?;
                        }

                        None => (),
                    };
                    visited_write_statement = true;
                    self.active_role = Some(Role::Primary);
                }
            };
        }

        Ok(())
    }

    fn handle_inferred_shard(
        &mut self,
        inferred_shard: Option<usize>,
        prev_inferred_shard: &mut Option<usize>,
    ) -> Result<(), Error> {
        if let Some(shard) = inferred_shard {
            if let Some(prev_shard) = *prev_inferred_shard {
                if prev_shard != shard {
                    debug!("Found more than one shard in the query, not supported yet");
                    return Err(Error::QueryRouterParserError(
                        "multiple shards in query".into(),
                    ));
                }
            }
            *prev_inferred_shard = Some(shard);
            self.active_shard = Some(shard);
            debug!("Automatically using shard: {:?}", self.active_shard);
        };
        Ok(())
    }

    fn table_mutations_cache(&self) -> Cache<String, bool> {
        TABLE_MUTATIONS_CACHE
            .get_or_init(|| {
                Cache::builder()
                    .time_to_live(Duration::from_millis(
                        self.pool_settings.table_mutation_cache_ms_ttl,
                    ))
                    .build()
            })
            .clone()
    }

    fn query_handles_tables_in_mutation_cache(&self, query: &sqlparser::ast::Query) -> bool {
        let table_mutations_cache = self.table_mutations_cache();
        debug!("Checking if query handles tables in mutation cache");
        debug!("Table mutations cache: {:?}", table_mutations_cache);

        for tables in self.table_names(query) {
            for table in tables {
                if table_mutations_cache.contains_key(&self.table_mutation_cache_key(table)) {
                    return true;
                }
            }
        }

        false
    }
    fn extract_exprs_and_table_names<'a>(
        &'a self,
        q: &'a Statement,
    ) -> Option<ExtractedExprsAndTables<'a>> {
        let mut exprs = Vec::new();
        let mut table_names = Vec::new();
        let mut assignments_opt = None;

        match q {
            Insert(i) => {
                // Not supported in postgres, which rejects them; the parser
                // accepts `INSERT OR ...`.
                if i.or.is_some() || i.partitioned.is_some() || !i.after_columns.is_empty() {
                    return None;
                }

                let TableObject::TableName(table_name) = &i.table else {
                    return None;
                };
                Self::process_table(table_name, &mut table_names);
                if let Some(source) = &i.source {
                    // Columns are plain names in postgres.
                    let columns = i
                        .columns
                        .iter()
                        .map(Self::single_ident)
                        .collect::<Option<Vec<_>>>()?;
                    Self::process_query(source, &mut exprs, &mut table_names, &Some(&columns));
                }
            }
            Delete(d) => {
                if let Some(expr) = &d.selection {
                    exprs.push(expr.clone());
                }

                // Multi-tables delete are not supported in postgres, which
                // rejects them.
                if !d.tables.is_empty() {
                    return None;
                }

                if let Some(using_tbl_with_join) = &d.using {
                    Self::process_tables_with_join(
                        using_tbl_with_join,
                        &mut exprs,
                        &mut table_names,
                    );
                }
                Self::process_selection(&d.selection, &mut exprs);
            }
            Update(update) => {
                Self::process_table_with_join(&update.table, &mut exprs, &mut table_names);
                match &update.from {
                    // Postgres takes FROM after SET only.
                    Some(UpdateTableFromKind::AfterSet(from_tbls)) => {
                        // One table, as the parser read FROM before 0.53.
                        let [from_tbl] = from_tbls.as_slice() else {
                            return None;
                        };
                        Self::process_table_with_join(from_tbl, &mut exprs, &mut table_names);
                    }
                    Some(UpdateTableFromKind::BeforeSet(_)) => return None,
                    None => (),
                }
                Self::process_selection(&update.selection, &mut exprs);

                assignments_opt = Some(&update.assignments);
            }
            _ => return None,
        };

        Some(ExtractedExprsAndTables {
            exprs,
            table_names,
            assignments_opt,
        })
    }

    fn infer_shard_on_write(&mut self, q: &Statement) -> Result<Option<usize>, Error> {
        if let Some(extracted) = self.extract_exprs_and_table_names(q) {
            let exprs = extracted.exprs;
            let table_names = extracted.table_names;
            let assignments_opt = extracted.assignments_opt;

            if let Some(assignments) = assignments_opt {
                self.assignment_parser(assignments)?;
            }

            Ok(self.infer_shard_from_exprs(exprs, table_names))
        } else {
            Ok(None)
        }
    }

    fn update_mutation_cache_on_write(&self, q: &Statement) {
        if let Some(extracted) = self.extract_exprs_and_table_names(q) {
            debug!("Updating mutation cache on write");

            let table_names = extracted.table_names;
            debug!("Table names in mutation query: {:?}", table_names);
            let table_mutations_cache = self.table_mutations_cache();
            for tables in table_names {
                for table in tables {
                    table_mutations_cache.insert(self.table_mutation_cache_key(table), true);
                }
            }
        }
    }

    // combines the database name and table name into a single string
    // to be used as the key in the table mutation cache
    // e.g. "mydb.mytable"
    fn table_mutation_cache_key(&self, table: Ident) -> String {
        format!("{}.{}", self.pool_settings.db, table.value)
    }

    fn process_query(
        query: &sqlparser::ast::Query,
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
        columns: &Option<&Vec<Ident>>,
    ) {
        match &*query.body {
            SetExpr::Query(query) => {
                Self::process_query(query, exprs, table_names, columns);
            }

            // SELECT * FROM ...
            // We understand that pretty well.
            SetExpr::Select(select) => {
                Self::process_tables_with_join(&select.from, exprs, table_names);

                // Parse the actual "FROM ..."
                Self::process_selection(&select.selection, exprs);
            }

            SetExpr::Values(values) => {
                if let Some(cols) = columns {
                    for row in values.rows.iter() {
                        for (i, expr) in row.iter().enumerate() {
                            if cols.len() > i {
                                exprs.push(Expr::BinaryOp {
                                    left: Box::new(Expr::Identifier(cols[i].clone())),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(expr.clone()),
                                });
                            }
                        }
                    }
                }
            }
            _ => (),
        };
    }

    fn process_selection(selection: &Option<Expr>, exprs: &mut Vec<Expr>) {
        match selection {
            Some(selection) => {
                exprs.push(selection.clone());
            }

            None => (),
        };
    }

    fn process_tables_with_join(
        tables: &[TableWithJoins],
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
    ) {
        for table in tables.iter() {
            Self::process_table_with_join(table, exprs, table_names);
        }
    }

    fn process_table_with_join(
        table: &TableWithJoins,
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
    ) {
        if let TableFactor::Table { name, .. } = &table.relation {
            Self::process_table(name, table_names);
        };

        // Get table names from all the joins.
        for join in table.joins.iter() {
            if let TableFactor::Table { name, .. } = &join.relation {
                Self::process_table(name, table_names);
            };

            // We can filter results based on join conditions, e.g.
            // SELECT * FROM t INNER JOIN B ON B.sharding_key = 5;
            // The parser reads a plain JOIN as `Join`, and INNER JOIN as `Inner`.
            if let JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::Inner(JoinConstraint::On(expr)) = &join.join_operator
            {
                // Parse the selection criteria later.
                exprs.push(expr.clone());
            };
        }
    }

    fn process_table(name: &ObjectName, table_names: &mut Vec<Vec<Ident>>) {
        if let Some(idents) = name
            .0
            .iter()
            .map(|part| part.as_ident().cloned())
            .collect::<Option<Vec<_>>>()
        {
            table_names.push(idents)
        }
    }

    /// The identifier of a one-part name.
    fn single_ident(name: &ObjectName) -> Option<Ident> {
        match name.0.as_slice() {
            [part] => part.as_ident().cloned(),
            _ => None,
        }
    }

    /// Parse the shard number from the Bind message
    /// which contains the arguments for a prepared statement.
    ///
    /// N.B.: Only supports anonymous prepared statements since we don't
    /// keep a cache of them in PgCat.
    pub fn infer_shard_from_bind(&mut self, message: &BytesMut) -> bool {
        if !self.pool_settings.query_parser_read_write_splitting {
            return false; // Nothing to do
        }

        debug!("Parsing bind message");

        let mut message_cursor = Cursor::new(message);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32();

        if code != 'B' {
            debug!("Not a bind packet");
            return false;
        }

        // Check message length
        if message.len() != len as usize + 1 {
            debug!(
                "Message has wrong length, expected {}, but have {}",
                len,
                message.len()
            );
            return false;
        }

        // There are no shard keys in the prepared statement.
        if self.placeholders.is_empty() {
            debug!("There are no placeholders in the prepared statement that matched the automatic sharding key");
            return false;
        }

        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );

        let mut shards = BTreeSet::new();

        let _portal = message_cursor.read_string();
        let _name = message_cursor.read_string();

        let num_params = message_cursor.get_i16();
        let parameter_format = match num_params {
            0 => ParameterFormat::Text, // Text
            1 => {
                let param_format = message_cursor.get_i16();
                ParameterFormat::Uniform(match param_format {
                    0 => Box::new(ParameterFormat::Text),
                    1 => Box::new(ParameterFormat::Binary),
                    _ => unreachable!(),
                })
            }
            n => {
                let mut v = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let param_format = message_cursor.get_i16();
                    v.push(match param_format {
                        0 => ParameterFormat::Text,
                        1 => ParameterFormat::Binary,
                        _ => unreachable!(),
                    });
                }
                ParameterFormat::Specified(v)
            }
        };

        let num_parameters = message_cursor.get_i16();

        for i in 0..num_parameters {
            let mut len = message_cursor.get_i32() as usize;
            let format = match &parameter_format {
                ParameterFormat::Text => ParameterFormat::Text,
                ParameterFormat::Uniform(format) => *format.clone(),
                ParameterFormat::Specified(formats) => formats[i as usize].clone(),
                _ => unreachable!(),
            };

            debug!("Parameter {} (len: {}): {:?}", i, len, format);

            // Postgres counts placeholders starting at 1
            let placeholder = i + 1;

            if self.placeholders.contains(&placeholder) {
                let value = match format {
                    ParameterFormat::Text => {
                        let mut value = String::new();
                        while len > 0 {
                            value.push(message_cursor.get_u8() as char);
                            len -= 1;
                        }

                        match value.parse::<i64>() {
                            Ok(value) => value,
                            Err(_) => {
                                debug!("Bind value of parameter {} isn't an integer", placeholder);
                                continue;
                            }
                        }
                    }

                    ParameterFormat::Binary => match len {
                        2 => message_cursor.get_i16() as i64,
                        4 => message_cursor.get_i32() as i64,
                        8 => message_cursor.get_i64(),
                        _ => {
                            error!(
                                "Got wrong length for integer type parameter in bind: {}",
                                len
                            );
                            continue;
                        }
                    },

                    _ => unreachable!(),
                };

                shards.insert(sharder.shard(value));
            }
        }

        self.placeholders.clear();
        self.placeholders.shrink_to_fit();

        // We only support querying one shard at a time.
        // TODO: Support multi-shard queries some day.
        if shards.len() == 1 {
            debug!("Found one sharding key");
            self.set_shard(Some(*shards.first().unwrap()));
            true
        } else {
            debug!("Found no sharding keys");
            false
        }
    }

    /// An `assignments` exists in the `UPDATE` statements. This parses the assignments and makes
    /// sure that we are not updating the sharding key. It's not supported yet.
    fn assignment_parser(&self, assignments: &Vec<Assignment>) -> Result<(), Error> {
        let sharding_key = self
            .pool_settings
            .automatic_sharding_key
            .as_ref()
            .unwrap()
            .split('.')
            .map(|ident| Ident::new(ident.to_lowercase()))
            .collect::<Vec<Ident>>();

        // Sharding key must be always fully qualified
        assert_eq!(sharding_key.len(), 2);

        for a in assignments {
            if sharding_key[0].value == "*"
                && sharding_key[1].value
                    == a.target
                        .to_string()
                        .split('.')
                        .last()
                        .unwrap()
                        .to_lowercase()
            {
                return Err(Error::QueryRouterParserError(
                    "Sharding key cannot be updated.".into(),
                ));
            }
        }
        Ok(())
    }

    /// A `selection` is the `WHERE` clause. This parses
    /// the clause and extracts the sharding key, if present.
    fn selection_parser(&self, expr: &Expr, table_names: &Vec<Vec<Ident>>) -> Vec<ShardingKey> {
        let mut result = Vec::new();
        let mut found = false;

        let sharding_key = self
            .pool_settings
            .automatic_sharding_key
            .as_ref()
            .unwrap()
            .split('.')
            .map(|ident| Ident::new(ident.to_lowercase()))
            .collect::<Vec<Ident>>();

        // Sharding key must be always fully qualified
        assert_eq!(sharding_key.len(), 2);

        // This parses `sharding_key = 5`. But it's technically
        // legal to write `5 = sharding_key`. I don't judge the people
        // who do that, but I think ORMs will still use the first variant,
        // so we can leave the second as a TODO.
        if let Expr::BinaryOp { left, op, right } = expr {
            match &**left {
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(left, table_names)),
                Expr::Identifier(ident) => {
                    // Only if we're dealing with only one table
                    // and there is no ambiguity
                    if ident.value.to_lowercase() == sharding_key[1].value {
                        // Sharding key is unique enough, don't worry about
                        // table names.
                        if &sharding_key[0].value == "*" {
                            found = true;
                        } else if table_names.len() == 1 {
                            let table = &table_names[0];

                            if table.len() == 1 {
                                // Table is not fully qualified, e.g.
                                //      SELECT * FROM t WHERE sharding_key = 5
                                // Make sure the table name from the sharding key matches
                                // the table name from the query.
                                found = sharding_key[0].value == table[0].value.to_lowercase();
                            } else if table.len() == 2 {
                                // Table name is fully qualified with the schema: e.g.
                                //      SELECT * FROM public.t WHERE sharding_key = 5
                                // Ignore the schema (TODO: at some point, we want schema support)
                                // and use the table name only.
                                found = sharding_key[0].value == table[1].value.to_lowercase();
                            } else {
                                debug!("Got table name with more than two idents, which is not possible");
                            }
                        }
                    }
                }

                Expr::CompoundIdentifier(idents) => {
                    // The key is fully qualified in the query,
                    // it will exist or Postgres will throw an error.
                    if idents.len() == 2 {
                        found = (&sharding_key[0].value == "*"
                            || sharding_key[0].value == idents[0].value.to_lowercase())
                            && sharding_key[1].value == idents[1].value.to_lowercase();
                    }
                    // TODO: key can have schema as well, e.g. public.data.id (len == 3)
                }
                _ => (),
            };

            match op {
                BinaryOperator::Eq => (),
                BinaryOperator::Or => (),
                BinaryOperator::And => (),
                _ => {
                    // TODO: support other operators than equality.
                    debug!("Unsupported operation: {:?}", op);
                    return Vec::new();
                }
            };

            match &**right {
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(right, table_names)),
                Expr::Value(ValueWithSpan {
                    value: Value::Number(value, ..),
                    ..
                }) => {
                    if found {
                        match value.parse::<i64>() {
                            Ok(value) => result.push(ShardingKey::Value(value)),
                            Err(_) => {
                                debug!("Sharding key was not an integer");
                            }
                        };
                    }
                }

                Expr::Value(ValueWithSpan {
                    value: Value::Placeholder(placeholder),
                    ..
                }) => match placeholder.replace('$', "").parse::<i16>() {
                    Ok(placeholder) => result.push(ShardingKey::Placeholder(placeholder)),
                    Err(_) => {
                        debug!(
                            "Prepared statement didn't have integer placeholders: {}",
                            placeholder
                        );
                    }
                },
                _ => (),
            };
        }

        // The keys are the query's literals.
        if !self.redact_queries() {
            debug!("Sharding keys found: {:?}", result);
        }

        result
    }

    /// Try to figure out which shard the query should go to.
    fn infer_shard(&mut self, query: &sqlparser::ast::Query) -> Option<usize> {
        let mut exprs = Vec::new();

        // Collect all table names from the query.
        let mut table_names = Vec::new();

        Self::process_query(query, &mut exprs, &mut table_names, &None);
        self.infer_shard_from_exprs(exprs, table_names)
    }

    /// get table names from query
    fn table_names(&self, query: &sqlparser::ast::Query) -> Vec<Vec<Ident>> {
        let mut exprs = Vec::new();

        let mut table_names = Vec::new();
        Self::process_query(query, &mut exprs, &mut table_names, &None);

        debug!("Table names in query: {:?}", table_names);

        table_names
    }

    fn infer_shard_from_exprs(
        &mut self,
        exprs: Vec<Expr>,
        table_names: Vec<Vec<Ident>>,
    ) -> Option<usize> {
        let mut shards = BTreeSet::new();

        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );

        // Look for sharding keys in either the join condition
        // or the selection.
        for expr in exprs.iter() {
            let sharding_keys = self.selection_parser(expr, &table_names);

            // TODO: Add support for prepared statements here.
            // This should just give us the position of the value in the `B` message.

            for value in sharding_keys {
                match value {
                    ShardingKey::Value(value) => {
                        let shard = sharder.shard(value);
                        shards.insert(shard);
                    }

                    ShardingKey::Placeholder(position) => {
                        self.placeholders.push(position);
                    }
                };
            }
        }
        match shards.len() {
            // Didn't find a sharding key, you're on your own.
            0 => {
                debug!("No sharding keys found");
                None
            }

            1 => Some(shards.into_iter().last().unwrap()),

            // TODO: support querying multiple shards (some day...)
            _ => {
                debug!("More than one sharding key found");
                None
            }
        }
    }

    /// Ask the plugins to verify an identity token the client set. With no
    /// plugin that verifies tokens, the token is kept as it is, without a tenant.
    /// Config validation allows one verifier per pool; should a pool have more,
    /// or a section its plugin hasn't parsed, the token is refused, unless the
    /// test-only `testing` verifier is on, which takes priority.
    ///
    /// A configured verifier runs like a plugin hook: under the pool's
    /// `plugin_hook_timeout` and `general.plugin_hook_concurrency`.
    ///
    /// An error is a refusal (28000), unless it has a SQLSTATE: the hook's
    /// `PH001` (timed out) or `PH002` (no slot).
    pub async fn verify_identity(&self, token: &IdentityToken) -> Result<VerifiedIdentity, String> {
        self.verify_identity_in(&crate::plugins::HOOK_SLOTS, token, Verifying::Set)
            .await
    }

    /// [`Self::verify_identity`], for the identity a client holds after a
    /// reload changed its verifier: a refusal is counted as the 0A000 that
    /// ends the session, not 28000.
    pub async fn verify_identity_again(
        &self,
        token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        self.verify_identity_in(&crate::plugins::HOOK_SLOTS, token, Verifying::Again)
            .await
    }

    /// [`Self::verify_identity`], with a configured verifier holding one of
    /// `slots` while it runs.
    pub(crate) async fn verify_identity_in(
        &self,
        slots: &tokio::sync::Semaphore,
        token: &IdentityToken,
        verifying: Verifying,
    ) -> Result<VerifiedIdentity, String> {
        #[cfg(any(test, feature = "test-plugins"))]
        if let Some(testing) = self
            .pool_settings
            .plugins
            .as_ref()
            .and_then(|plugins| plugins.testing.as_ref())
        {
            if testing.enabled && testing.verify_identity {
                return counted(
                    "testing",
                    verifying,
                    crate::plugins::testing::verify_identity(token),
                );
            }
        }

        let mut verifier = None;
        for (name, section) in self.named_configured_sections() {
            match section {
                Section::On(plugin, config) if plugin.verifies_identity(config) => {
                    if verifier.replace((name, plugin, config)).is_some() {
                        return Err(MANY_VERIFIERS.into());
                    }
                }
                Section::On(..) | Section::Off => {}
                Section::NotLoaded => return Err(registry::NOT_LOADED.into()),
            }
        }

        match verifier {
            Some((name, plugin, config)) => {
                let verify = async { Ok(plugin.verify_identity(config, token).await) };
                let timeout = self.pool_settings.plugin_hook_timeout;
                let verified = match crate::plugins::run_hook_with(slots, timeout, verify).await {
                    // A refusal: the client gets the message with 28000, so
                    // it can't pick a SQLSTATE of its own, or hold the token.
                    Ok(verified) => verified.map_err(|message| {
                        let (_, message) = crate::messages::sqlstate_of(&message);
                        // With no NUL left, client.rs can't read a SQLSTATE
                        // out of it, however many the plugin nested.
                        let refusal = message.replace('\0', " ");
                        // The whole token is looked for too, as a plugin's may
                        // have other characters; before and after, so a NUL
                        // can't hide it.
                        if might_quote_token(&refusal)
                            || message.contains(token.as_str())
                            || refusal.contains(token.as_str())
                        {
                            REDACTED_REFUSAL.to_string()
                        } else {
                            refusal
                        }
                    }),
                    // Timed out or no hook slot: the hook's SQLSTATE (PH001,
                    // PH002), not a refusal's 28000.
                    Err(error) => Err(error.to_string()),
                };
                counted(name, verifying, verified)
            }
            None => Ok(VerifiedIdentity::default()),
        }
    }

    /// Identifies what verifies identity tokens on the pool, as
    /// [`Self::verify_identity`] finds it, with its section: 0 if nothing
    /// does. A client's identity verified with another is verified again
    /// before its next statement (see `Identity::recheck`).
    pub fn verifier_key(&self) -> u64 {
        use std::hash::{Hash, Hasher};

        let plugins = match self.pool_settings.plugins.as_ref() {
            Some(plugins) => plugins,
            None => return 0,
        };
        #[cfg(any(test, feature = "test-plugins"))]
        let testing = plugins
            .testing
            .as_ref()
            .map_or(false, |testing| testing.enabled && testing.verify_identity);
        #[cfg(not(any(test, feature = "test-plugins")))]
        let testing = false;
        // A section no plugin parsed refuses every token.
        let sections: Vec<_> = plugins
            .configured
            .iter()
            .filter(|(name, section)| {
                match Section::resolve(self.registered_plugins.configured_plugin(name), section) {
                    Section::On(plugin, config) => plugin.verifies_identity(config),
                    Section::Off => false,
                    Section::NotLoaded => true,
                }
            })
            .collect();
        if !testing && sections.is_empty() {
            return 0;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        testing.hash(&mut hasher);
        sections.hash(&mut hasher);
        hasher.finish() | 1
    }

    /// Run the plugins on a simple Query.
    pub async fn execute_plugins(&self, ast: &Vec<Statement>) -> Result<PluginOutput, Error> {
        Ok(self.execute_query_plugins(ast, None).await?.output)
    }

    /// Run the plugins on a simple Query, keeping the plan for its results.
    pub async fn execute_query_plugins(
        &self,
        ast: &Vec<Statement>,
        identity: Option<&Identity>,
    ) -> Result<PluginResult, Error> {
        self.execute_plugin_chain(ast, None, identity).await
    }

    /// Run the plugins on a Parse. `param_types` are the types the client declared.
    pub async fn execute_parse_plugins(
        &self,
        ast: &Vec<Statement>,
        param_types: &[i32],
        identity: Option<&Identity>,
    ) -> Result<PluginResult, Error> {
        self.execute_plugin_chain(ast, Some(param_types), identity)
            .await
    }

    /// Add your plugins here and execute them.
    ///
    /// Registered plugins run first, then `query_logger`, `intercept`,
    /// `table_access`, the newer built-ins, and the configured plugins the pool
    /// turns on. Every plugin sees the original statements. After them, the
    /// final rewrite is checked (`check_rewrite`), another plugin's as parsed
    /// again from its SQL: by `table_access`, so a rewrite cannot reach a
    /// blocked table, and by each configured plugin that checks rewrites, so
    /// another plugin's rewrite cannot reach what it covers.
    /// The first `Deny` or `Intercept` wins and stops the chain. At most one
    /// plugin may return `Overwrite` (or a statement plan); a second one denies the
    /// query, since the two can't be combined safely.
    ///
    /// `parse` holds the declared parameter types when this is a Parse.
    async fn execute_plugin_chain(
        &self,
        ast: &Vec<Statement>,
        parse: Option<&[i32]>,
        identity: Option<&Identity>,
    ) -> Result<PluginResult, Error> {
        let no_builtins = crate::config::Plugins::default();
        let plugins = match self.pool_settings.plugins {
            Some(ref plugins) => plugins,
            None if !self.registered_plugins.has_process_plugins() => {
                return Ok(PluginOutput::Allow.into())
            }
            None => &no_builtins,
        };

        // External plugins see every parsed statement before a legacy intercept
        // can answer it. They use the existing single-plan/single-rewrite rule.
        let mut merged = PluginResult::from(PluginOutput::Allow);
        let ctx = PluginContext {
            query_router: self,
            declared_param_types: parse.unwrap_or(&[]),
            identity,
        };
        let hook = match parse {
            Some(_) => Hook::Parse,
            None => Hook::Query,
        };
        self.note(NO_PLUGIN);
        for (name, plugin) in self.registered_plugins.plugins() {
            self.note(name);
            // An error stops the chain: `chain_failed` counts it.
            let result = match parse {
                Some(_) => plugin.on_parse(&ctx, ast).await?,
                None => plugin.on_query(&ctx, ast).await?,
            };
            if let Some(output) = Self::merge_from(name, hook, Ok(result), &mut merged) {
                return Ok(output.into());
            }
        }

        if let Some(ref query_logger) = plugins.query_logger {
            self.note("query_logger");
            let mut query_logger = QueryLogger {
                enabled: query_logger.enabled && !self.redact_queries(),
                user: &self.pool_settings.user.username,
                db: &self.pool_settings.db,
            };

            let _ = query_logger.run(self, ast).await;
        }

        if let Some(ref intercept) = plugins.intercept {
            self.note("intercept");
            let mut intercept = Intercept {
                enabled: intercept.enabled,
                config: intercept,
            };

            let result = intercept.run(self, ast).await;

            if let Ok(PluginOutput::Intercept(output)) = result {
                return Ok(PluginOutput::Intercept(output).into());
            }
        }

        if let Some(ref table_access) = plugins.table_access {
            self.note("table_access");
            let mut table_access = TableAccess {
                enabled: table_access.enabled,
                tables: &table_access.tables,
            };

            let result = table_access.run(self, ast).await;

            if let Ok(PluginOutput::Deny(error)) = result {
                return Ok(PluginOutput::Deny(error).into());
            }
        }

        // Newer plugins. Unlike the ones above, an error from these denies the query.
        #[cfg(any(test, feature = "test-plugins"))]
        if let Some(ref testing) = plugins.testing {
            self.note("testing");
            let mut testing = crate::plugins::testing::Testing {
                enabled: testing.enabled,
                config: testing,
            };

            let result = match parse {
                Some(_) => testing.on_parse(&ctx, ast).await,
                None => testing.on_query(&ctx, ast).await,
            };

            if let Some(output) = Self::merge_from("testing", hook, result, &mut merged) {
                return Ok(output.into());
            }
        }

        // Configured plugins the pool turns on, in registration order.
        // `rewriter` is the configured plugin whose rewrite the chain carries,
        // and whether its rewrite is checked as it built it.
        let mut rewriter = None;
        for (name, section) in self.named_configured_sections() {
            let result = match section {
                Section::Off => continue,
                Section::NotLoaded => Ok(PluginOutput::Deny(registry::not_loaded()).into()),
                Section::On(plugin, config) => {
                    self.note(name);
                    let result = plugin.run(config, &ctx, ast, parse.is_some()).await;
                    if let Ok(PluginResult {
                        output: PluginOutput::Overwrite(_),
                        ..
                    }) = &result
                    {
                        rewriter = Some((name, plugin.rewrite_checked_as_built(config)));
                    }
                    result
                }
            };

            if let Some(output) = Self::merge_from(name, hook, result, &mut merged) {
                return Ok(output.into());
            }
        }

        if let PluginOutput::Overwrite(ref statements) = merged.output {
            let table_access = plugins.table_access.as_ref();
            if let Some(output) = self.check_rewrite(table_access, statements, rewriter).await {
                return Ok(output.into());
            }
        }

        Ok(merged)
    }

    /// Check the final rewrite after all plugins ran. Another plugin's rewrite
    /// is checked as the server will read it: its SQL, parsed again, since a
    /// plugin's statements can print as SQL that names other tables than they
    /// hold. It's parsed once, and only if a check below applies. `rewriter`
    /// is the configured plugin that rewrote, if one did, and whether its
    /// rewrite is checked as it built it. Returns the output that refuses the
    /// rewrite, if one does.
    async fn check_rewrite(
        &self,
        table_access: Option<&crate::config::TableAccess>,
        rewrite: &Vec<Statement>,
        rewriter: Option<(&'static str, bool)>,
    ) -> Option<PluginOutput> {
        // A configured plugin plans the client's statements, not another
        // plugin's rewrite of them, so each one on the pool that checks
        // rewrites gets to refuse it. Its own rewrite is the one it planned.
        let rewriter_name = rewriter.map(|(name, _)| name);
        let checkers = || {
            self.named_configured_sections()
                .filter_map(move |(name, section)| match section {
                    Section::On(plugin, config)
                        if Some(name) != rewriter_name && plugin.checks_rewrites(config) =>
                    {
                        Some((plugin, config))
                    }
                    _ => None,
                })
        };
        // table_access checks both sides of a rewrite: the original SQL in the
        // chain, so a query naming a blocked table can't be rewritten away from
        // it, and the final SQL here, so a rewrite can't reach a blocked table.
        let table_access = table_access.filter(|table_access| table_access.enabled);
        if table_access.is_none() && checkers().next().is_none() {
            return None;
        }

        // A rewrite checked as built is checked from the plugin's own AST:
        // built from the client's parsed statements and names it quotes
        // itself, it can hold SQL that sqlparser prints but can't parse.
        // Another plugin's rewrite is parsed again; what can't be parsed can't
        // be checked, so that fails closed.
        let parsed;
        let sent: &Vec<Statement> = if matches!(rewriter, Some((_, true))) {
            rewrite
        } else {
            parsed = match self.parse_rewrite(&crate::plugins::statements_to_sql(rewrite)) {
                Ok(parsed) => parsed,
                Err(err) => {
                    // The parser's errors are fixed text, without the SQL.
                    debug!("refused a plugin's rewrite: {}", err);
                    return Some(PluginOutput::Deny(crate::messages::with_sqlstate(
                        crate::plugins::PLUGIN_FAILED,
                        "a plugin's rewrite can't be parsed",
                    )));
                }
            };
            &parsed
        };

        for (plugin, config) in checkers() {
            if let Err(error) = plugin.check_rewrite(config, sent) {
                return Some(PluginOutput::Deny(error));
            }
        }

        if let Some(table_access) = table_access {
            self.note("table_access");
            let mut table_access = TableAccess {
                enabled: true,
                tables: &table_access.tables,
            };

            // Anything but Allow refuses the rewrite.
            if !matches!(table_access.run(self, sent).await, Ok(PluginOutput::Allow)) {
                // The client didn't write the rewritten SQL, so the error doesn't
                // name the table the rewrite reached: it can be plugin-derived,
                // for example per tenant.
                return Some(PluginOutput::Deny(crate::messages::with_sqlstate(
                    crate::plugins::PLUGIN_DENIED,
                    "permission denied for rewritten statement",
                )));
            }
        }

        None
    }

    /// The plugin chain calls `plugin` next.
    fn note(&self, plugin: &'static str) {
        *self.running.lock() = plugin;
    }

    /// The plugin the plugin chain last called.
    pub(crate) fn running_plugin(&self) -> &'static str {
        *self.running.lock()
    }

    /// The message a client gets when the plugin chain on `hook` fails with
    /// `err`, the error a registered plugin returned or the chain's hook
    /// timeout (PH001) or slot (PH002), counted for the plugin it called last.
    pub(crate) fn chain_failed(&self, hook: Hook, err: &Error) -> String {
        let message = crate::messages::or_sqlstate(crate::plugins::PLUGIN_FAILED, &err.to_string());
        hook_errors::count(self.running_plugin(), hook, &message);
        message
    }

    /// [`Self::merge_plugin_result`] for `plugin`'s result: its plan is
    /// stamped with the plugin's name, and its error, which denies the
    /// statement (42501), counted.
    fn merge_from(
        plugin: &'static str,
        hook: Hook,
        mut result: Result<PluginResult, Error>,
        merged: &mut PluginResult,
    ) -> Option<PluginOutput> {
        match &mut result {
            Ok(PluginResult {
                plan: Some(plan), ..
            }) => plan.plugin = plugin,
            Ok(_) => {}
            Err(_) => hook_errors::count_code(plugin, hook, Some(crate::plugins::PLUGIN_DENIED)),
        }
        Self::merge_plugin_result(result, merged)
    }

    /// Fold one plugin's result into the chain. Returns `Some` if the chain
    /// should stop with that output.
    #[allow(dead_code)]
    fn merge_plugin_result(
        result: Result<PluginResult, Error>,
        merged: &mut PluginResult,
    ) -> Option<PluginOutput> {
        let result = match result {
            Ok(result) => result,
            Err(err) => return Some(PluginOutput::Deny(format!("plugin error: {}", err))),
        };

        // Every plugin sees the original AST. A plan from one plugin cannot
        // safely describe SQL rewritten by a different plugin.
        if (result.plan.is_some() && matches!(merged.output, PluginOutput::Overwrite(_)))
            || (matches!(result.output, PluginOutput::Overwrite(_)) && merged.plan.is_some())
        {
            return Some(PluginOutput::Deny(crate::messages::with_sqlstate(
                crate::plugins::PLUGIN_DENIED,
                "statement transformed by more than one plugin",
            )));
        }

        if result.plan.is_some() {
            if merged.plan.is_some() {
                error!("More than one plugin planned the same statement");
                return Some(PluginOutput::Deny(
                    "statement planned by more than one plugin".into(),
                ));
            }
            merged.plan = result.plan;
        }

        match result.output {
            PluginOutput::Allow => None,
            PluginOutput::Overwrite(statements) => {
                if let PluginOutput::Overwrite(_) = merged.output {
                    error!("More than one plugin rewrote the same query");
                    return Some(PluginOutput::Deny(
                        "query rewritten by more than one plugin".into(),
                    ));
                }
                merged.output = PluginOutput::Overwrite(statements);
                None
            }
            output => Some(output),
        }
    }

    /// Queries may hold values that must not be logged: with a configured
    /// plugin that says so, SQL text, literals and parse errors (which quote
    /// the query) stay out of the logs. So they do if a section isn't loaded.
    pub fn redact_queries(&self) -> bool {
        self.configured_sections().any(|section| match section {
            Section::On(plugin, config) => plugin.sensitive_queries(config),
            Section::Off => false,
            Section::NotLoaded => true,
        })
    }

    /// The text of the server's errors and notices is withheld from clients:
    /// a configured plugin that's on says so. So it is if a section isn't
    /// loaded.
    pub fn withholds_server_text(&self) -> bool {
        self.configured_sections().any(|section| match section {
            Section::On(plugin, config) => plugin.withholds_server_text(config),
            Section::Off => false,
            Section::NotLoaded => true,
        })
    }

    /// The pool's `[plugins.<name>]` sections: those of registered configured
    /// plugins in registration order, then any no plugin is registered for.
    /// It runs for every statement, so it doesn't allocate.
    fn configured_sections(&self) -> impl Iterator<Item = Section<'_>> + '_ {
        self.named_configured_sections().map(|(_, section)| section)
    }

    /// [`Self::configured_sections`], with each one's plugin name:
    /// `NO_PLUGIN` for those no plugin is registered for.
    fn named_configured_sections(&self) -> impl Iterator<Item = (&'static str, Section<'_>)> + '_ {
        let sections = self
            .pool_settings
            .plugins
            .as_ref()
            .map(|plugins| &plugins.configured)
            .filter(|sections| !sections.is_empty());
        let registered = sections.into_iter().flat_map(move |sections| {
            self.registered_plugins
                .configured_plugins()
                .filter_map(move |(name, plugin)| {
                    sections
                        .get(name)
                        .map(|section| (name, Section::resolve(Some(plugin), section)))
                })
        });
        let unregistered = sections
            .into_iter()
            .flatten()
            .filter(move |(name, _)| self.registered_plugins.configured_plugin(name).is_none())
            .map(|(_, section)| (NO_PLUGIN, Section::resolve(None, section)));
        registered.chain(unregistered)
    }

    /// Identifies the plugin configuration statements are planned with: 0 if
    /// no plugin that plans them is on. A statement planned with another
    /// generation must be planned again.
    pub fn plan_generation(&self) -> u64 {
        self.plan_generation
    }

    fn compute_plan_generation(&self) -> u64 {
        use std::hash::{Hash, Hasher};

        if !self.plugins_enabled() {
            return 0;
        }
        let sections: Vec<_> = match self.pool_settings.plugins {
            Some(ref plugins) => plugins
                .configured
                .iter()
                .filter(|(name, section)| {
                    !matches!(
                        Section::resolve(self.registered_plugins.configured_plugin(name), section),
                        Section::Off
                    )
                })
                .collect(),
            None => Vec::new(),
        };
        if sections.is_empty() {
            return 0;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sections.hash(&mut hasher);
        hasher.finish() | 1
    }

    /// Registered code runs independently of pool TOML and parser routing
    /// settings. Legacy plugins retain their configured parser requirement.
    pub fn plugins_enabled(&self) -> bool {
        self.registered_plugins.has_process_plugins()
            || (self.pool_settings.plugins.is_some() && self.pool_settings.query_parser_enabled)
    }

    /// Queries that can't be parsed must be rejected instead of forwarded,
    /// because a plugin needs to see every query.
    pub fn plugins_require_parse(&self) -> bool {
        self.registered_plugins.has_process_plugins()
            || (self.plugins_enabled() && self.pool_settings.plugins_require_parse)
    }

    /// External policy is opaque to the core, so no unparsed query can bypass
    /// it. Nor can one bypass a configured plugin that's on, unless it allows
    /// unparsed queries outside its gated tables. Legacy plugins, and those
    /// that allow them, retain their table-scoped compatibility exceptions.
    pub fn rejects_unparsed(&self, message: &BytesMut) -> bool {
        if self.registered_plugins.has_process_plugins() {
            return true;
        }
        // A section no plugin parsed can't say what it checks.
        if self.plugins_enabled()
            && self
                .configured_sections()
                .any(|section| matches!(section, Section::NotLoaded))
        {
            return true;
        }
        let every = self.refuses_every_unparsed();
        if !every && !self.plugins_require_parse() {
            return false;
        }

        let mut cursor = Cursor::new(message);
        let code = cursor.get_u8() as char;
        let _len = cursor.get_i32();

        if code == 'P' && cursor.read_string().is_err() {
            return true;
        }

        match cursor.read_string() {
            Ok(query) => {
                !TABLE_FREE_UNPARSED_REGEX.is_match(&query)
                    && (every || self.might_touch_gated(&query))
            }
            Err(_) => true,
        }
    }

    /// A configured plugin that's on sees every statement that reaches the
    /// server, unless it allows unparsed ones outside its gated tables (see
    /// `ConfiguredPlugin::allows_unparsed_outside_gated_tables`).
    fn refuses_every_unparsed(&self) -> bool {
        self.plugins_enabled()
            && self.configured_sections().any(|section| match section {
                Section::On(plugin, config) => !plugin.allows_unparsed_outside_gated_tables(config),
                Section::Off | Section::NotLoaded => false,
            })
    }

    /// A query the parser couldn't read might touch a table a plugin checks:
    /// its text has a name a plugin gates (a table's, or a function's that
    /// runs SQL given as text, which can build any name), or Unicode escapes
    /// (`U&"..."`), which can spell any name. A VACUUM without a table name
    /// processes every table, so any VACUUM might, as might a `DO` block or a
    /// function or procedure whose body is a string.
    fn might_touch_gated(&self, query: &str) -> bool {
        let query = query.to_ascii_lowercase();
        let gated = self.gated_tables();
        query.contains("u&")
            || (!gated.is_empty() && first_word(&query) == "vacuum")
            || (!gated.is_empty() && runs_string_body(&query))
            || gated
                .iter()
                .any(|table| query.contains(&table.to_ascii_lowercase()))
    }

    /// Names plugins gate: the tables they check statements on, and others an
    /// unparsed query mustn't mention (see `ConfiguredPlugin::gated_tables`).
    fn gated_tables(&self) -> Vec<&str> {
        let mut tables = Vec::new();
        if let Some(ref plugins) = self.pool_settings.plugins {
            if let Some(ref table_access) = plugins.table_access {
                if table_access.enabled {
                    tables.extend(table_access.tables.iter().map(String::as_str));
                }
            }
        }
        for section in self.configured_sections() {
            if let Section::On(plugin, config) = section {
                tables.extend(plugin.gated_tables(config));
            }
        }
        tables
    }

    fn set_sharding_key(&mut self, sharding_key: i64) -> Option<usize> {
        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );
        let shard = sharder.shard(sharding_key);
        self.set_shard(Some(shard));
        self.active_shard
    }

    /// Set active_role as the default_role specified in the pool.
    pub fn set_default_role(&mut self) {
        self.active_role = self.pool_settings.default_role;
    }

    /// Get the current desired server role we should be talking to.
    pub fn role(&self) -> Option<Role> {
        self.active_role
    }

    /// Get desired shard we should be talking to.
    pub fn shard(&self) -> Option<usize> {
        self.active_shard
    }

    pub fn set_shard(&mut self, shard: Option<usize>) {
        self.active_shard = shard;
    }

    /// Should we attempt to parse queries?
    pub fn query_parser_enabled(&self) -> bool {
        match self.query_parser_enabled {
            None => {
                debug!(
                    "Using pool settings, query_parser_enabled: {}",
                    self.pool_settings.query_parser_enabled
                );
                self.pool_settings.query_parser_enabled
            }

            Some(value) => {
                debug!(
                    "Using query parser override, query_parser_enabled: {}",
                    value
                );
                value
            }
        }
    }

    pub fn primary_reads_enabled(&self) -> bool {
        match self.primary_reads_enabled {
            None => self.pool_settings.primary_reads_enabled,
            Some(value) => value,
        }
    }
}

impl Default for QueryRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::PoolMode;
    use crate::messages::simple_query;
    use crate::sharding::ShardingFunction;
    use bytes::BufMut;
    use serial_test::serial;

    #[test]
    fn test_might_quote_token() {
        let twenty = "aZ09-_".repeat(4)[..20].to_string();
        assert!(might_quote_token(&twenty));
        assert!(might_quote_token(&format!("bad token: {}.", twenty)));
        assert!(!might_quote_token(&twenty[..19]));
        // Other characters end a run.
        for separator in [".", "+", "/", "=", " ", "\0", "é"] {
            let split = format!("{}{}{}", &twenty[..10], separator, &twenty[10..]);
            assert!(!might_quote_token(&split), "{:?}", split);
        }
    }

    #[test]
    fn test_defaults() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_split_cte_queries() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;

        let query = simple_query(
            "WITH t AS (
                SELECT id FROM users WHERE name ILIKE '%ja%'
            )
            UPDATE user_languages
               SET settings = '{}'
              FROM t WHERE t.id = user_id;",
        );
        let ast = qr.parse(&query).unwrap();
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));
    }

    #[test]
    fn test_data_modifying_bodies_and_ctes_go_to_primary() {
        QueryRouter::setup();
        let role_of = |sql: &str| {
            let mut qr = QueryRouter::new();
            qr.pool_settings.query_parser_read_write_splitting = true;
            qr.pool_settings.primary_reads_enabled = false;
            assert!(qr.infer(&qr.parse(&simple_query(sql)).unwrap()).is_ok());
            qr.role()
        };

        for sql in [
            // The parser reads these as a query whose body is the DELETE or
            // MERGE.
            "WITH x AS (SELECT 1) DELETE FROM data WHERE id = 5",
            "WITH x AS (SELECT 1) MERGE INTO data USING a ON data.id = a.id \
             WHEN MATCHED THEN DELETE",
            // A SELECT over a CTE that writes.
            "WITH x AS (DELETE FROM data WHERE id = 5 RETURNING *) SELECT * FROM x",
            "WITH x AS (UPDATE data SET v = 1 RETURNING *) SELECT * FROM x",
            "WITH x AS (INSERT INTO data VALUES (1) RETURNING *) SELECT * FROM x",
            "WITH r AS (SELECT 1), x AS (DELETE FROM data RETURNING *) SELECT * FROM r",
            "WITH x AS (WITH y AS (DELETE FROM data RETURNING *) SELECT * FROM y) \
             SELECT * FROM x",
            "SELECT 1 UNION ALL (WITH x AS (DELETE FROM data RETURNING *) SELECT 1)",
            // A read after one of these, or after a locking read, in the same
            // message stays on the primary.
            "WITH x AS (SELECT 1) DELETE FROM data WHERE id = 5; SELECT 1",
            "WITH x AS (SELECT 1) MERGE INTO data USING a ON data.id = a.id \
             WHEN MATCHED THEN DELETE; SELECT 1",
            "WITH x AS (DELETE FROM data RETURNING *) SELECT * FROM x; SELECT 1",
            "SELECT * FROM data FOR UPDATE; SELECT 1",
        ] {
            assert_eq!(role_of(sql), Some(Role::Primary), "{}", sql);
        }

        for sql in [
            "SELECT 1; SELECT 1",
            "WITH x AS (SELECT * FROM data) SELECT * FROM x",
            "WITH x AS (WITH y AS (SELECT 1) SELECT * FROM y) SELECT * FROM x",
            "SELECT 1 UNION ALL (WITH x AS (SELECT 1) SELECT * FROM x)",
        ] {
            assert_eq!(role_of(sql), Some(Role::Replica), "{}", sql);
        }
    }

    #[test]
    fn test_infer_replica() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;
        assert!(qr
            .try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'"))
            .is_some());
        assert!(qr.query_parser_enabled());

        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        let queries = vec![
            simple_query("SELECT * FROM items WHERE id = 5"),
            simple_query(
                "SELECT id, name, value FROM items INNER JOIN prices ON item.id = prices.item_id",
            ),
            simple_query("WITH t AS (SELECT * FROM items) SELECT * FROM t"),
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Replica));
        }
    }

    #[test]
    fn test_infer_primary() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let queries = vec![
            simple_query("UPDATE items SET name = 'pumpkin' WHERE id = 5"),
            simple_query("INSERT INTO items (id, name) VALUES (5, 'pumpkin')"),
            simple_query("DELETE FROM items WHERE id = 5"),
            simple_query("BEGIN"), // Transaction start
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Primary));
        }
    }

    #[test]
    fn test_select_for_update() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let queries_in_primary_role = vec![
            simple_query("BEGIN"), // Transaction start
            simple_query("SELECT * FROM items WHERE id = 5 FOR UPDATE"),
            simple_query("UPDATE items SET name = 'pumpkin' WHERE id = 5"),
        ];

        for query in queries_in_primary_role {
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Primary));
        }

        // query without lock do not change role
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_primary_reads_enabled() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO on"))
            .is_some());

        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_parse_prepared() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        qr.try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'"));
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        let prepared_stmt = BytesMut::from(
            &b"WITH t AS (SELECT * FROM items WHERE name = $1) SELECT * FROM t WHERE id = $2\0"[..],
        );
        let mut res = BytesMut::from(&b"P"[..]);
        res.put_i32(prepared_stmt.len() as i32 + 4 + 1 + 2);
        res.put_u8(0);
        res.put(prepared_stmt);
        res.put_i16(0);

        assert!(qr.infer(&qr.parse(&res).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Replica));
    }

    #[test]
    fn test_regex_set() {
        QueryRouter::setup();

        let tests = [
            // Upper case
            "SET SHARDING KEY TO '1'",
            "SET SHARD TO '1'",
            "SHOW SHARD",
            "SET SERVER ROLE TO 'replica'",
            "SET SERVER ROLE TO 'primary'",
            "SET SERVER ROLE TO 'any'",
            "SET SERVER ROLE TO 'auto'",
            "SHOW SERVER ROLE",
            "SET PRIMARY READS TO 'on'",
            "SET PRIMARY READS TO 'off'",
            "SET PRIMARY READS TO 'default'",
            "SHOW PRIMARY READS",
            // Lower case
            "set sharding key to '1'",
            "set shard to '1'",
            "show shard",
            "set server role to 'replica'",
            "set server role to 'primary'",
            "set server role to 'any'",
            "set server role to 'auto'",
            "show server role",
            "set primary reads to 'on'",
            "set primary reads to 'OFF'",
            "set primary reads to 'deFaUlt'",
            // No quotes
            "SET SHARDING KEY TO 11235",
            "SET SHARD TO 15",
            "SET PRIMARY READS TO off",
            // Spaces and semicolon
            "  SET SHARDING KEY TO 11235  ; ",
            "  SET SHARD TO 15;   ",
            "  SET SHARDING KEY TO 11235  ;",
            " SET SERVER ROLE TO 'primary';   ",
            "    SET SERVER ROLE TO 'primary'  ; ",
            "  SET SERVER ROLE TO 'primary'  ;",
            "  SET PRIMARY READS TO 'off'    ;",
        ];

        // Which regexes it'll match to in the list
        let matches = [
            0, 1, 2, 3, 3, 3, 3, 4, 5, 5, 5, 6, 0, 1, 2, 3, 3, 3, 3, 4, 5, 5, 5, 0, 1, 5, 0, 1, 0,
            3, 3, 3, 5,
        ];

        let list = CUSTOM_SQL_REGEX_LIST.get().unwrap();
        let set = CUSTOM_SQL_REGEX_SET.get().unwrap();

        for (i, test) in tests.iter().enumerate() {
            if !list[matches[i]].is_match(test) {
                println!("{} does not match {}", test, list[matches[i]]);
                panic!();
            }
            assert_eq!(set.matches(test).into_iter().count(), 1);
        }

        let bad = [
            "SELECT * FROM table",
            "SELECT * FROM table WHERE value = 'set sharding key to 5'", // Don't capture things in the middle of the query
        ];

        for query in &bad {
            assert_eq!(set.matches(query).into_iter().count(), 0);
        }
    }

    #[test]
    fn test_try_execute_command() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        // SetShardingKey
        let query = simple_query("SET SHARDING KEY TO 13");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::SetShardingKey, String::from("0")))
        );
        assert_eq!(qr.shard().unwrap(), 0);

        // SetShard
        let query = simple_query("SET SHARD TO '1'");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::SetShard, String::from("1")))
        );
        assert_eq!(qr.shard().unwrap(), 1);

        // ShowShard
        let query = simple_query("SHOW SHARD");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::ShowShard, String::from("1")))
        );

        // SetServerRole
        let roles = ["primary", "replica", "any", "auto", "primary"];
        let verify_roles = [
            Some(Role::Primary),
            Some(Role::Replica),
            None,
            None,
            Some(Role::Primary),
        ];
        let query_parser_enabled = [false, false, false, true, false];

        for (idx, role) in roles.iter().enumerate() {
            let query = simple_query(&format!("SET SERVER ROLE TO '{}'", role));
            assert_eq!(
                qr.try_execute_command(&query),
                Some((Command::SetServerRole, String::from(*role)))
            );
            assert_eq!(qr.role(), verify_roles[idx],);
            assert_eq!(qr.query_parser_enabled(), query_parser_enabled[idx],);

            // ShowServerRole
            let query = simple_query("SHOW SERVER ROLE");
            assert_eq!(
                qr.try_execute_command(&query),
                Some((Command::ShowServerRole, String::from(*role)))
            );
        }

        let primary_reads = ["on", "off", "default"];
        let primary_reads_enabled = ["on", "off", "on"];

        for (idx, primary_reads) in primary_reads.iter().enumerate() {
            assert_eq!(
                qr.try_execute_command(&simple_query(&format!(
                    "SET PRIMARY READS TO {}",
                    primary_reads
                ))),
                Some((Command::SetPrimaryReads, String::from(*primary_reads)))
            );
            assert_eq!(
                qr.try_execute_command(&simple_query("SHOW PRIMARY READS")),
                Some((
                    Command::ShowPrimaryReads,
                    String::from(primary_reads_enabled[idx])
                ))
            );
        }
    }

    #[test]
    fn test_enable_query_parser() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let query = simple_query("SET SERVER ROLE TO 'auto'");
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        assert!(qr.try_execute_command(&query).is_some());
        assert!(qr.query_parser_enabled());
        assert_eq!(qr.role(), None);

        let query = simple_query("INSERT INTO test_table VALUES (1)");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));

        let query = simple_query("SELECT * FROM test_table");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Replica));

        assert!(qr.query_parser_enabled());
        let query = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&query).is_some());
        assert!(!qr.query_parser_enabled());
    }

    #[test]
    fn test_query_parser() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let query = simple_query("SELECT req_tab_0.*  FROM validation req_tab_0  WHERE  array['http://www.w3.org/ns/shacl#ValidationResult'] && req_tab_0.type::text[] AND ( (  (req_tab_0.focusnode = 'DataSource_Credilogic_DataSourceAddress_144959227') )  )");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());

        let query = simple_query("WITH EmployeeSalaries AS (SELECT Department, Salary FROM Employees) SELECT Department, AVG(Salary) AS AverageSalary FROM EmployeeSalaries GROUP BY Department;");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
    }

    #[test]
    fn test_update_from_pool_settings() {
        QueryRouter::setup();

        let pool_settings = PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: crate::config::LoadBalancingMode::Random,
            checkout_failure_limit: None,
            shards: 2,
            user: crate::config::User::default(),
            default_role: Some(Role::Replica),
            query_parser_enabled: true,
            query_parser_max_length: None,
            query_parser_read_write_splitting: true,
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: Some(String::from("test.id")),
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: None,
            shard_id_regex: None,
            default_shard: crate::config::DefaultShard::Shard(0),
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
            db: "test".to_string(),
            db_activity_based_routing: PoolSettings::default().db_activity_based_routing,
            db_activity_init_delay: PoolSettings::default().db_activity_init_delay,
            db_activity_ttl: PoolSettings::default().db_activity_ttl,
            table_mutation_cache_ms_ttl: PoolSettings::default().table_mutation_cache_ms_ttl,
            plugins: None,
            ..Default::default()
        };
        let mut qr = QueryRouter::new();
        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert_eq!(qr.query_parser_enabled, None);
        assert_eq!(qr.primary_reads_enabled, None);

        // Internal state must not be changed due to this, only defaults
        qr.update_pool_settings(&pool_settings);

        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert!(qr.query_parser_enabled());
        assert!(!qr.primary_reads_enabled());

        let q1 = simple_query("SET SERVER ROLE TO 'primary'");
        assert!(qr.try_execute_command(&q1).is_some());
        assert_eq!(qr.active_role.unwrap(), Role::Primary);

        let q2 = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&q2).is_some());
        assert_eq!(qr.active_role.unwrap(), pool_settings.default_role);
    }

    #[test]
    fn test_parse_multiple_queries() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        assert!(qr
            .infer(&qr.parse(&simple_query("BEGIN; SELECT 1; COMMIT;")).unwrap())
            .is_ok());
        assert_eq!(qr.role(), Role::Primary);

        assert!(qr
            .infer(&qr.parse(&simple_query("SELECT 1; SELECT 2;")).unwrap())
            .is_ok());
        assert_eq!(qr.role(), Role::Replica);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT 123; INSERT INTO t VALUES (5); SELECT 1;"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.role(), Role::Primary);
    }

    #[test]
    fn test_regex_shard_parsing() {
        QueryRouter::setup();

        let pool_settings = PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: crate::config::LoadBalancingMode::Random,
            checkout_failure_limit: Some(10),
            shards: 5,
            user: crate::config::User::default(),
            default_role: Some(Role::Replica),
            query_parser_enabled: true,
            query_parser_max_length: None,
            query_parser_read_write_splitting: true,
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: Some(Regex::new(r"/\* sharding_key: (\d+) \*/").unwrap()),
            shard_id_regex: Some(Regex::new(r"/\* shard_id: (\d+) \*/").unwrap()),
            default_shard: crate::config::DefaultShard::Shard(0),
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
            db: "test".to_string(),
            db_activity_based_routing: PoolSettings::default().db_activity_based_routing,
            db_activity_init_delay: PoolSettings::default().db_activity_init_delay,
            db_activity_ttl: PoolSettings::default().db_activity_ttl,
            table_mutation_cache_ms_ttl: PoolSettings::default().table_mutation_cache_ms_ttl,
            plugins: None,
            ..Default::default()
        };

        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);

        // Shard should start out unset
        assert_eq!(qr.active_shard, None);

        // Don't panic when short query eg. ; is sent
        let q0 = simple_query(";");
        assert!(qr.try_execute_command(&q0).is_none());
        assert_eq!(qr.active_shard, None);

        // Make sure setting it works
        let q1 = simple_query("/* shard_id: 1 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q1).is_none());
        assert_eq!(qr.active_shard, Some(1));

        // And make sure changing it works
        let q2 = simple_query("/* shard_id: 0 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2).is_none());
        assert_eq!(qr.active_shard, Some(0));

        // Validate setting by shard with expected shard copied from sharding.rs tests
        let q2 = simple_query("/* sharding_key: 6 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2).is_none());
        assert_eq!(qr.active_shard, Some(2));
    }

    #[test]
    fn test_automatic_sharding_key() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        assert!(qr
            .infer(
                &qr.parse(&simple_query("SELECT * FROM data WHERE id = 5"))
                    .unwrap(),
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT one, two, three FROM public.data WHERE id = 6"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM data
            INNER JOIN t2 ON data.id = 5
            AND t2.data_id = data.id
        WHERE data.id = 5"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        // Shard did not move because we couldn't determine the sharding key since it could be ambiguous
        // in the query.
        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM t2 INNER JOIN data ON id = 6 AND data.id = t2.data_id"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    r#"SELECT * FROM "public"."data" WHERE "id" = 6"#
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    r#"SELECT * FROM "public"."data" WHERE "data"."id" = 5"#
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        // Super unique sharding key
        qr.pool_settings.automatic_sharding_key = Some("*.unique_enough_column_name".to_string());
        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM table_x WHERE unique_enough_column_name = 6"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query("SELECT * FROM table_y WHERE another_key = 5"))
                    .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);
    }

    #[test]
    fn test_automatic_sharding_key_in_join_on() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        let mut shard_of = |sql: &str| {
            assert!(qr.infer(&qr.parse(&simple_query(sql)).unwrap()).is_ok());
            qr.shard()
        };

        // The key is only in ON. Each query moves to a different shard than
        // the previous one, so a join whose ON isn't read keeps the previous
        // shard.
        assert_eq!(shard_of("SELECT * FROM data WHERE id = 6"), Some(0));
        assert_eq!(
            shard_of("SELECT * FROM data JOIN t2 ON data.id = 5 AND t2.data_id = data.id"),
            Some(2)
        );
        assert_eq!(shard_of("SELECT * FROM data WHERE id = 6"), Some(0));
        assert_eq!(
            shard_of("SELECT * FROM data INNER JOIN t2 ON data.id = 5 AND t2.data_id = data.id"),
            Some(2)
        );
        assert_eq!(
            shard_of("SELECT * FROM t2 JOIN data ON data.id = 6 AND t2.data_id = data.id"),
            Some(0)
        );

        // An outer join's ON doesn't filter the rows of both tables, so it
        // still doesn't choose a shard.
        assert_eq!(
            shard_of("SELECT * FROM data LEFT JOIN t2 ON data.id = 5 AND t2.data_id = data.id"),
            Some(0)
        );
    }

    #[test]
    fn test_writes_postgres_rejects_are_not_inferred() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        // The parser accepts these, and the server rejects them. Inference
        // skips them instead of panicking.
        for sql in [
            "INSERT OR REPLACE INTO data (id) VALUES (5)",
            "DELETE data FROM data WHERE id = 5",
        ] {
            assert!(
                qr.infer(&qr.parse(&simple_query(sql)).unwrap()).is_ok(),
                "{}",
                sql
            );
            assert_eq!(qr.role(), Some(Role::Primary), "{}", sql);
            assert_eq!(qr.shard(), None, "{}", sql);
        }
    }

    fn auto_shard_wrapper(qry: &str, should_succeed: bool) -> Option<usize> {
        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("*.w_id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;
        assert_eq!(qr.shard(), None);
        let infer_res = qr.infer(&qr.parse(&simple_query(qry)).unwrap());
        assert_eq!(infer_res.is_ok(), should_succeed);
        qr.shard()
    }

    fn auto_shard(qry: &str) -> Option<usize> {
        auto_shard_wrapper(qry, true)
    }

    fn auto_shard_fails(qry: &str) -> Option<usize> {
        auto_shard_wrapper(qry, false)
    }

    #[test]
    fn test_automatic_sharding_insert_update_delete() {
        QueryRouter::setup();

        assert_eq!(
            auto_shard_fails(
                "UPDATE ORDERS SET w_id = 3 WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"
            ),
            None
        );

        assert_eq!(
            auto_shard_fails(
                "UPDATE ORDERS o SET o.W_ID = 3 WHERE o.O_ID = 3 AND o.O_D_ID = 3 AND o.W_ID = 5"
            ),
            None
        );

        assert_eq!(
            auto_shard(
                "UPDATE ORDERS o SET o.O_CARRIER_ID = 3 WHERE o.O_ID = 3 AND o.O_D_ID = 3 AND o.W_ID = 5"
            ),
            Some(2)
        );
    }

    #[test]
    fn test_automatic_sharding_key_tpcc() {
        QueryRouter::setup();

        assert_eq!(auto_shard("SELECT * FROM my_tbl WHERE w_id = 5"), Some(2));
        assert_eq!(
            auto_shard("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
            None
        );
        assert_eq!(auto_shard("COMMIT"), None);
        assert_eq!(auto_shard("ROLLBACK"), None);

        assert_eq!(auto_shard("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = 7 AND W_ID = 5 AND NO_O_ID > 3 LIMIT 3"), Some(2));
        assert_eq!(auto_shard("SELECT NO_O_ID FROM NEW_ORDER no WHERE no.NO_D_ID = 7 AND no.W_ID = 5 AND no.NO_O_ID > 3 LIMIT 3"), Some(2));

        assert_eq!(
            auto_shard("DELETE FROM NEW_ORDER WHERE NO_D_ID = 7 AND W_ID = 5 AND NO_O_ID = 3"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT O_C_ID FROM ORDERS WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard(
                "UPDATE ORDERS SET O_CARRIER_ID = 3 WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"
            ),
            Some(2)
        );

        assert_eq!(
            auto_shard("UPDATE ORDER_LINE SET OL_DELIVERY_D = 3 WHERE OL_O_ID = 3 AND OL_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = 3 AND OL_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + 3 WHERE C_ID = 3 AND C_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE DISTRICT SET D_NEXT_O_ID = 3 WHERE D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO ORDERS (O_ID, O_D_ID, W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (3, 3, 5, 3, 3, 3, 3, 3)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, W_ID) VALUES (3, 3, 5)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 3"),
            None
        );
        assert_eq!(
            auto_shard("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE STOCK SET S_QUANTITY = 3, S_YTD = 3, S_ORDER_CNT = 3, S_REMOTE_CNT = 3 WHERE S_I_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (3, 3, 5, 3, 3, 3, 3, 3, 3, 3)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_LAST = '3' ORDER BY C_FIRST"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE W_ID = 5 AND O_D_ID = 3 AND O_C_ID = 3 ORDER BY O_ID DESC LIMIT 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE W_ID = 5 AND OL_D_ID = 3 AND OL_O_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE WAREHOUSE SET W_YTD = W_YTD + 3 WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE W_ID = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE DISTRICT SET D_YTD = D_YTD + 3 WHERE W_ID  = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_LAST = '3' ORDER BY C_FIRST"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = 3, C_YTD_PAYMENT = 3, C_PAYMENT_CNT = 3, C_DATA = 3 WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = 3, C_YTD_PAYMENT = 3, C_PAYMENT_CNT = 3 WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );

        assert_eq!(auto_shard("INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (3, 3, 5, 3, 5, 3, 3, 3)"), Some(2));
        assert_eq!(
            auto_shard("SELECT D_NEXT_O_ID FROM DISTRICT WHERE W_ID = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard(
                "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
                 WHERE ORDER_LINE.W_ID = 5
                 AND OL_D_ID = 3
                 AND OL_O_ID < 3
                 AND OL_O_ID >= 3
                 AND STOCK.W_ID = 5
                 AND S_I_ID = OL_I_ID
                 AND S_QUANTITY < 3"
            ),
            Some(2)
        );

        // This is a distributed query and contains two shards
        assert_eq!(
            auto_shard(
                "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
                 WHERE ORDER_LINE.W_ID = 5
                 AND OL_D_ID = 3
                 AND OL_O_ID < 3
                 AND OL_O_ID >= 3
                 AND STOCK.W_ID = 7
                 AND S_I_ID = OL_I_ID
                 AND S_QUANTITY < 3"
            ),
            None
        );
    }

    #[test]
    fn test_prepared_statements() {
        let stmt = "SELECT * FROM data WHERE id = $1";

        let mut bind = BytesMut::from(&b"B"[..]);

        let mut payload = BytesMut::from(&b"\0\0"[..]);
        payload.put_i16(0);
        payload.put_i16(1);
        payload.put_i32(1);
        payload.put(&b"5"[..]);
        payload.put_i16(0);

        bind.put_i32(payload.len() as i32 + 4);
        bind.put(payload);

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        assert!(qr.infer(&qr.parse(&simple_query(stmt)).unwrap()).is_ok());
        assert_eq!(qr.placeholders.len(), 1);

        assert!(qr.infer_shard_from_bind(&bind));
        assert_eq!(qr.shard().unwrap(), 2);
        assert!(qr.placeholders.is_empty());
    }

    /// A router with the table_access plugin forbidding `secrets`, and
    /// plugins_require_parse on.
    fn forbidding_secrets() -> QueryRouter {
        use crate::config::{Plugins, TableAccess};
        QueryRouter::setup();
        let pool_settings = PoolSettings {
            query_parser_enabled: true,
            plugins_require_parse: true,
            plugins: Some(Plugins {
                table_access: Some(TableAccess {
                    enabled: true,
                    tables: vec![String::from("secrets")],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);
        qr
    }

    #[test]
    fn test_parse_fuzz() {
        use crate::fuzz::Rng;
        let qr = forbidding_secrets();
        let pieces: [&[u8]; 6] = [
            b"SELECT 1",
            b"\0",
            b"s",
            b"COPY secrets FROM STDIN",
            b";",
            b"\xff",
        ];
        for seed in 0..5_000 {
            let mut rng = Rng::new(seed);
            let mut body = Vec::new();
            for _ in 0..rng.below(5) {
                match rng.one_in(3) {
                    true => body.extend(rng.bytes(4)),
                    false => body.extend(*rng.pick(&pieces)),
                }
            }
            let mut message = BytesMut::new();
            message.put_u8(*rng.pick(b"QP"));
            message.put_i32(body.len() as i32 + 4);
            message.put_slice(&body);
            // Whatever the bytes, each returns.
            let _ = qr.parse(&message);
            let _ = qr.rejects_unparsed(&message);
        }
    }

    #[tokio::test]
    async fn test_forbidden_table_fuzz() {
        use crate::fuzz::Rng;
        let qr = forbidding_secrets();
        let spellings = [
            "secrets",
            "SECRETS",
            "Secrets",
            "\"secrets\"",
            "public.secrets",
            "public.\"secrets\"",
            "\"public\".secrets",
        ];
        let statements = [
            "SELECT * FROM {t}",
            "SELECT a FROM {t} WHERE b = 1",
            "SELECT * FROM other JOIN {t} ON true",
            "SELECT * FROM (SELECT * FROM {t}) s",
            "WITH w AS (SELECT * FROM {t}) SELECT * FROM w",
            "SELECT 1 UNION SELECT a FROM {t}",
            "SELECT (SELECT a FROM {t} LIMIT 1)",
            "SELECT * FROM other WHERE a IN (SELECT a FROM {t})",
            "INSERT INTO {t} VALUES (1)",
            "INSERT INTO other SELECT * FROM {t}",
            "UPDATE {t} SET a = 1",
            "DELETE FROM {t}",
            "COPY {t} TO STDOUT",
            "COPY (SELECT * FROM {t}) TO STDOUT",
            "COPY {t} FROM STDIN",
            "TABLE {t}",
            "SELECT * FROM ONLY {t}",
            "EXPLAIN ANALYZE SELECT * FROM {t}",
            "CREATE TABLE copied AS SELECT * FROM {t}",
        ];
        let spaces = [" ", "  ", "\n", "\t", " /* c */ ", " -- c\n"];

        for seed in 0..5_000 {
            let mut rng = Rng::new(seed);
            let statement = rng.pick(&statements).replace("{t}", rng.pick(&spellings));
            let mut sql = String::new();
            let mut quoted = false;
            for c in statement.chars() {
                quoted ^= c == '"';
                match c {
                    ' ' => sql.push_str(rng.pick(&spaces)),
                    // Outside quotes, Postgres folds names and keywords to lower case.
                    _ if !quoted && rng.one_in(2) => sql.push(c.to_ascii_uppercase()),
                    _ => sql.push(c),
                }
            }

            // Refused unread, or read and denied: never forwarded unchecked.
            let message = simple_query(&sql);
            match qr.parse(&message) {
                Ok(ast) => assert!(
                    matches!(qr.execute_plugins(&ast).await, Ok(PluginOutput::Deny(_))),
                    "seed {}: {:?}",
                    seed,
                    sql
                ),
                Err(_) => assert!(qr.rejects_unparsed(&message), "seed {}: {:?}", seed, sql),
            }
        }
    }

    #[tokio::test]
    async fn test_only_checks_the_real_table() {
        let qr = forbidding_secrets();
        for sql in [
            "SELECT * FROM ONLY secrets",
            "SELECT * FROM ONLY (secrets)",
            "SELECT * FROM other JOIN ONLY public.secrets s ON true",
            "SELECT * FROM secrets *",
            "UPDATE ONLY Secrets SET a = 1",
            "DELETE FROM ONLY secrets",
            "MERGE INTO ONLY secrets USING other ON true WHEN MATCHED THEN DELETE",
        ] {
            let ast = qr.parse(&simple_query(sql)).expect(sql);
            assert!(
                matches!(qr.execute_plugins(&ast).await, Ok(PluginOutput::Deny(_))),
                "{}",
                sql
            );
        }
        let ast = qr.parse(&simple_query("SELECT * FROM ONLY other")).unwrap();
        assert_eq!(qr.execute_plugins(&ast).await.unwrap(), PluginOutput::Allow);
    }

    #[tokio::test]
    async fn test_statements_after_copy_from_stdin_are_checked() {
        let qr = forbidding_secrets();
        // The server runs the INSERT after the COPY in each of these.
        for sql in [
            "COPY labels FROM STDIN; INSERT INTO secrets VALUES (1)",
            "COPY labels FROM STDIN;\nINSERT INTO secrets VALUES (1)",
            "COPY labels FROM STDIN; SELECT 1;\nINSERT INTO secrets VALUES (1)",
            "COPY labels FROM STDIN;\n\nINSERT INTO secrets VALUES (1)",
        ] {
            // Refused unread, or read and denied: never forwarded unchecked.
            let message = simple_query(sql);
            match qr.parse(&message) {
                Ok(ast) => assert!(
                    matches!(qr.execute_plugins(&ast).await, Ok(PluginOutput::Deny(_))),
                    "{:?}",
                    sql
                ),
                Err(_) => assert!(qr.rejects_unparsed(&message), "{:?}", sql),
            }
        }
        // A CRLF ends the line too: refused unread.
        let message = simple_query("COPY labels FROM STDIN;\r\nINSERT INTO secrets VALUES (1)");
        assert!(qr.parse(&message).is_err());
        assert!(qr.rejects_unparsed(&message));
        // The parser reads SQL again after `\.`, and after a comment across the
        // line break: read and denied.
        for sql in [
            "COPY labels FROM STDIN;\n\\.\nINSERT INTO secrets VALUES (1)",
            "COPY labels FROM STDIN; /*\n*/ INSERT INTO secrets VALUES (1)",
        ] {
            let ast = qr.parse(&simple_query(sql)).expect(sql);
            assert!(
                matches!(qr.execute_plugins(&ast).await, Ok(PluginOutput::Deny(_))),
                "{:?}",
                sql
            );
        }
        // Text on a new line is refused whatever it names.
        for sql in [
            "COPY labels FROM STDIN;\nSELECT 1",
            "COPY labels FROM STDIN;\n-- c",
        ] {
            assert!(QueryRouter::parse_sql(sql).is_err(), "{:?}", sql);
        }
        // Blank text isn't.
        for sql in [
            "COPY labels FROM STDIN",
            "COPY labels FROM STDIN;\n",
            "COPY labels FROM STDIN;\n\n",
            "COPY labels FROM STDIN;\n    ",
        ] {
            assert!(QueryRouter::parse_sql(sql).is_ok(), "{:?}", sql);
        }
    }

    #[tokio::test]
    async fn test_partitions_and_parents_are_checked() {
        let qr = forbidding_secrets();
        let run = |sql: &str| {
            let ast = qr.parse(&simple_query(sql)).expect(sql);
            let qr = &qr;
            async move { qr.execute_plugins(&ast).await.unwrap() }
        };
        for sql in [
            // Reads secrets through p.
            "CREATE TABLE p (LIKE secrets) PARTITION BY LIST (a); \
             ALTER TABLE p ATTACH PARTITION secrets DEFAULT; SELECT * FROM p",
            "ALTER TABLE p ATTACH PARTITION public.secrets FOR VALUES IN (1); SELECT * FROM p",
            "ALTER TABLE p ATTACH PARTITION \"secrets\" DEFAULT",
            "ALTER TABLE p ATTACH PARTITION secrets FOR VALUES WITH (MODULUS 1, REMAINDER 0)",
            "ALTER TABLE p DETACH PARTITION secrets CONCURRENTLY",
            "ALTER TABLE p INHERIT secrets",
            "ALTER TABLE IF EXISTS ONLY p ADD COLUMN b int, NO INHERIT Secrets",
            "ALTER FOREIGN TABLE p INHERIT secrets",
            "CREATE FOREIGN TABLE f (a int) INHERITS (other, secrets) SERVER s",
            "CREATE FUNCTION f() RETURNS void LANGUAGE sql \
             BEGIN ATOMIC ALTER TABLE p INHERIT secrets; END",
            // Rows inserted into f are read from secrets.
            "CREATE TABLE f INHERITS (secrets)",
            "CREATE TABLE f (a int) INHERITS (other, public.\"secrets\")",
            "CREATE UNLOGGED TABLE f () INHERITS (other, public.secrets)",
            "CREATE UNLOGGED TABLE f (a int) INHERITS (secrets); INSERT INTO f VALUES (7)",
            "DROP FOREIGN TABLE secrets",
            "DROP FOREIGN TABLE IF EXISTS other, secrets CASCADE",
        ] {
            assert!(matches!(run(sql).await, PluginOutput::Deny(_)), "{}", sql);
        }
        for sql in [
            "ALTER TABLE p ATTACH PARTITION other DEFAULT",
            "ALTER TABLE p ATTACH PARTITION \"Secrets\" DEFAULT",
            "ALTER TABLE p INHERIT other",
            "CREATE FOREIGN TABLE f (a int) INHERITS (other) SERVER s",
            "CREATE TABLE f INHERITS (other)",
            "CREATE UNLOGGED TABLE f () INHERITS (other)",
            "DROP FOREIGN TABLE other",
        ] {
            assert_eq!(run(sql).await, PluginOutput::Allow, "{}", sql);
        }
    }

    #[test]
    fn test_nested_calls_parse_in_bounded_time() {
        use std::sync::mpsc;
        use std::time::Duration;
        let nested = |open: &str, close: &str, depth: usize| {
            format!("SELECT {}1{}", open.repeat(depth), close.repeat(depth))
        };
        for (sql, parses) in [
            (nested("f(", ")", 30), true),
            (nested("ROW(", ")", 30), true),
            (nested("POSITION(", ")", 30), true),
            // Past the parser's recursion limit: an error, but a quick one.
            (nested("ARRAY[", "]", 48), false),
        ] {
            let (tx, rx) = mpsc::channel();
            let query = sql.clone();
            // A slow parse is left running: the test fails without waiting for it.
            std::thread::spawn(move || {
                let _ = tx.send(QueryRouter::parse_sql(&query).is_ok());
            });
            let result = rx.recv_timeout(Duration::from_secs(1));
            assert_eq!(result, Ok(parses), "{}", &sql[..20]);
        }
    }

    #[tokio::test]
    async fn test_table_access_plugin() {
        use crate::config::{Plugins, TableAccess};
        let table_access = TableAccess {
            enabled: true,
            tables: vec![String::from("pg_database")],
        };
        let plugins = Plugins {
            table_access: Some(table_access),
            intercept: None,
            query_logger: None,
            prewarmer: None,
            ..Default::default()
        };

        QueryRouter::setup();
        let pool_settings = PoolSettings {
            query_parser_enabled: true,
            plugins: Some(plugins),
            ..Default::default()
        };
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);

        let query = simple_query("SELECT * FROM pg_database");
        let ast = qr.parse(&query).unwrap();

        let res = qr.execute_plugins(&ast).await;

        assert_eq!(
            res,
            Ok(PluginOutput::Deny(
                "permission for table \"pg_database\" denied".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn test_table_access_checks_original_and_rewritten_statements() {
        use crate::config::{Plugins, TableAccess, Testing};

        let rewrites = [
            ("select * from allowed where false", "SELECT * FROM blocked"),
            ("select * from blocked where false", "SELECT * FROM allowed"),
            ("select * from other where false", "SELECT * FROM allowed"),
            // `COPY t` isn't visited as a relation; table_access checks it.
            ("select * from copied where false", "COPY blocked TO STDOUT"),
        ];
        let pool_settings = PoolSettings {
            query_parser_enabled: true,
            plugins: Some(Plugins {
                table_access: Some(TableAccess {
                    enabled: true,
                    tables: vec![String::from("blocked")],
                }),
                testing: Some(Testing {
                    enabled: true,
                    rewrites: rewrites
                        .iter()
                        .map(|(from, to)| (from.to_string(), to.to_string()))
                        .collect(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);
        let run = |sql: &str| {
            let ast = qr.parse(&simple_query(sql)).unwrap();
            let qr = &qr;
            async move { qr.execute_plugins(&ast).await.unwrap() }
        };
        let original_denial = PluginOutput::Deny("permission for table \"blocked\" denied".into());

        assert_eq!(run("SELECT * FROM blocked").await, original_denial);
        assert_eq!(run("SELECT * FROM allowed").await, PluginOutput::Allow);
        assert_eq!(
            run("SELECT * FROM blocked WHERE false").await,
            original_denial
        );
        assert_eq!(
            run("SELECT * FROM other WHERE false").await,
            PluginOutput::Overwrite(qr.parse(&simple_query("SELECT * FROM allowed")).unwrap())
        );
        for sql in [
            "SELECT * FROM allowed WHERE false",
            "SELECT * FROM copied WHERE false",
        ] {
            assert_eq!(
                run(sql).await,
                PluginOutput::Deny(crate::messages::with_sqlstate(
                    crate::plugins::PLUGIN_DENIED,
                    "permission denied for rewritten statement",
                )),
                "{}",
                sql
            );
        }
    }

    /// A plugin whose rewrite holds no table but prints as SQL that names one.
    struct Smuggles(&'static str);

    #[async_trait::async_trait]
    impl crate::plugins::registry::RegisteredPlugin for Smuggles {
        async fn on_query(
            &self,
            _context: &PluginContext<'_>,
            _statements: &[Statement],
        ) -> Result<PluginResult, Error> {
            use sqlparser::ast::{visit_expressions_mut, visit_relations, Expr};
            use std::ops::ControlFlow;

            let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT a").unwrap();
            let _ = visit_expressions_mut(&mut statements, |expr| {
                if let Expr::Identifier(ident) = expr {
                    ident.value = self.0.into();
                }
                ControlFlow::<()>::Continue(())
            });
            assert!(visit_relations(&statements, |_| ControlFlow::Break(())).is_continue());
            Ok(PluginOutput::Overwrite(statements).into())
        }
    }

    #[tokio::test]
    async fn test_rewrites_are_checked_as_the_sql_that_is_sent() {
        use crate::config::{Plugins, TableAccess};
        use crate::messages::with_sqlstate;
        use crate::plugins::registry::PluginRegistry;
        use crate::plugins::{statements_to_sql, PLUGIN_DENIED, PLUGIN_FAILED};

        let settings = PoolSettings {
            query_parser_enabled: true,
            plugins: Some(Plugins {
                table_access: Some(TableAccess {
                    enabled: true,
                    tables: vec!["blocked".into()],
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        QueryRouter::setup();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1").unwrap();
        for (smuggled, expected) in [
            // table_access sees the table.
            (
                "1 FROM blocked",
                Some((PLUGIN_DENIED, "permission denied for rewritten statement")),
            ),
            (
                "1 FROM",
                Some((PLUGIN_FAILED, "a plugin's rewrite can't be parsed")),
            ),
            ("1 FROM allowed", None),
        ] {
            let mut registry = PluginRegistry::default();
            registry.register("smuggles", Smuggles(smuggled)).unwrap();
            let mut qr = QueryRouter::with_plugins(std::sync::Arc::new(registry));
            qr.update_pool_settings(&settings);

            for parse in [false, true] {
                let result = match parse {
                    true => qr.execute_parse_plugins(&ast, &[], None).await,
                    false => qr.execute_query_plugins(&ast, None).await,
                };
                match (result.unwrap().output, expected) {
                    (PluginOutput::Deny(error), Some((code, message))) => {
                        assert_eq!(error, with_sqlstate(code, message), "{}", smuggled)
                    }
                    (PluginOutput::Overwrite(statements), None) => {
                        assert_eq!(statements_to_sql(&statements), "SELECT 1 FROM allowed")
                    }
                    (output, _) => panic!("{} (parse: {}): {:?}", smuggled, parse, output),
                }
            }
        }
    }

    fn testing_plugin_settings(require_parse: bool) -> PoolSettings {
        use crate::config::{Plugins, Testing};

        let testing = Testing {
            enabled: true,
            rewrites: [("select 1".to_string(), "SELECT 2".to_string())].into(),
            ..Default::default()
        };

        PoolSettings {
            query_parser_enabled: true,
            plugins_require_parse: require_parse,
            plugins: Some(Plugins {
                testing: Some(testing),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_plugin_overwrite_simple_query() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&testing_plugin_settings(false));

        let ast = qr.parse(&simple_query("SELECT 1")).unwrap();
        let statements = match qr.execute_plugins(&ast).await {
            Ok(PluginOutput::Overwrite(statements)) => statements,
            other => panic!("expected a rewrite, got {:?}", other),
        };

        // This is the Q message the client code sends instead.
        let rewritten = simple_query(&crate::plugins::statements_to_sql(&statements));
        assert_eq!(rewritten, simple_query("SELECT 2"));

        // Other queries are left alone.
        let ast = qr.parse(&simple_query("SELECT 3")).unwrap();
        assert_eq!(qr.execute_plugins(&ast).await, Ok(PluginOutput::Allow));
    }

    fn parse_message(name: &str, query: &str, param_types: &[i32]) -> BytesMut {
        let mut payload = BytesMut::new();
        payload.put_slice(format!("{}\0{}\0", name, query).as_bytes());
        payload.put_i16(param_types.len() as i16);
        for oid in param_types {
            payload.put_i32(*oid);
        }

        let mut message = BytesMut::from(&b"P"[..]);
        message.put_i32(payload.len() as i32 + 4);
        message.put(payload);
        message
    }

    #[tokio::test]
    async fn test_plugin_overwrite_parse() {
        use crate::messages::Parse;

        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&testing_plugin_settings(false));

        let message = parse_message("my_stmt", "SELECT 1", &[23, 25]);
        let ast = qr.parse(&message).unwrap();
        let statements = match qr.execute_plugins(&ast).await {
            Ok(PluginOutput::Overwrite(statements)) => statements,
            other => panic!("expected a rewrite, got {:?}", other),
        };

        // Same steps as the client code: swap the query, keep name and param OIDs.
        let parse: Parse = (&message).try_into().unwrap();
        let rewritten: BytesMut = parse
            .with_query(crate::plugins::statements_to_sql(&statements))
            .try_into()
            .unwrap();

        assert_eq!(rewritten, parse_message("my_stmt", "SELECT 2", &[23, 25]));
    }

    #[tokio::test]
    async fn test_plugin_overwrite_shares_prepared_statement_cache() {
        use crate::messages::Parse;
        use crate::pool::PreparedStatementCache;

        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&testing_plugin_settings(false));

        let mut cache = PreparedStatementCache::new(10);
        let mut names = Vec::new();

        // Two clients with different statement names, and one that sends the
        // rewritten SQL directly, all share one server-side statement.
        for (name, query) in [("a", "SELECT 1"), ("b", "SELECT 1"), ("c", "SELECT 2")] {
            let message = parse_message(name, query, &[]);
            let ast = qr.parse(&message).unwrap();
            let parse: Parse = (&message).try_into().unwrap();
            let parse = match qr.execute_plugins(&ast).await {
                Ok(PluginOutput::Overwrite(statements)) => {
                    parse.with_query(crate::plugins::statements_to_sql(&statements))
                }
                _ => parse,
            };

            assert_eq!(parse.query(), "SELECT 2");
            names.push(cache.get_or_insert(&parse, parse.get_hash()).name.clone());
        }

        assert_eq!(names[0], names[1]);
        assert_eq!(names[0], names[2]);
    }

    #[tokio::test]
    async fn test_plugin_plans_bind() {
        use crate::config::TestingCopyParam;
        use crate::messages::Bind;
        use crate::plugins::ExecuteContext;

        QueryRouter::setup();
        let mut settings = testing_plugin_settings(false);
        if let Some(ref mut plugins) = settings.plugins {
            plugins.testing.as_mut().unwrap().copy_param = Some(TestingCopyParam {
                table: "t".into(),
                from: "a".into(),
                to: "b".into(),
            });
        }
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&settings);

        let message = parse_message("", "INSERT INTO t (c, a) VALUES ($2, $1), ($3, $4)", &[]);
        let ast = qr.parse(&message).unwrap();
        let result = qr.execute_parse_plugins(&ast, &[], None).await.unwrap();

        match result.output {
            PluginOutput::Overwrite(ref statements) => assert_eq!(
                crate::plugins::statements_to_sql(statements),
                "INSERT INTO t (c, a, b) VALUES ($2, $1, $5), ($3, $4, $6)"
            ),
            ref other => panic!("expected a rewrite, got {:?}", other),
        }

        let plan = result.plan.expect("a plan");
        assert_eq!(plan.client_params, 4);
        assert_eq!(plan.appended_param_types, vec![25, 25]);

        // Bind: c1, a1, c2, a2 in text format.
        let mut payload = BytesMut::from(&b"\0\0"[..]);
        payload.put_i16(0);
        payload.put_i16(4);
        for value in [&b"a1"[..], b"c1", b"c2", b"a2"] {
            payload.put_i32(value.len() as i32);
            payload.put_slice(value);
        }
        payload.put_i16(0);
        let mut bind = BytesMut::from(&b"B"[..]);
        bind.put_i32(payload.len() as i32 + 4);
        bind.put(payload);

        let bind: Bind = (&bind).try_into().unwrap();
        let mut params = bind.params();
        let mut ctx = ExecuteContext::new(qr.pool_settings(), None);
        plan.hooks
            .on_bind(&mut ctx, &plan, &mut params)
            .await
            .unwrap();

        assert!(params.modified());
        assert_eq!(params.len(), 6);
        assert_eq!(params.value(4), Some(&b"a1"[..]));
        assert_eq!(params.value(5), Some(&b"a2"[..]));
    }

    #[test]
    fn test_merge_plugin_result() {
        let mut merged = PluginResult::from(PluginOutput::Allow);
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 2").unwrap();
        let overwrite = || Ok(PluginOutput::Overwrite(statements.clone()).into());

        assert!(
            QueryRouter::merge_plugin_result(Ok(PluginOutput::Allow.into()), &mut merged).is_none()
        );
        assert!(QueryRouter::merge_plugin_result(overwrite(), &mut merged).is_none());
        assert_eq!(merged.output, PluginOutput::Overwrite(statements.clone()));

        // A second rewrite can't be combined with the first one.
        assert!(matches!(
            QueryRouter::merge_plugin_result(overwrite(), &mut merged),
            Some(PluginOutput::Deny(_))
        ));

        // Deny stops the chain, and so does a plugin error.
        let mut merged = PluginResult::from(PluginOutput::Allow);
        assert_eq!(
            QueryRouter::merge_plugin_result(
                Ok(PluginOutput::Deny("no".into()).into()),
                &mut merged
            ),
            Some(PluginOutput::Deny("no".into()))
        );
        assert!(matches!(
            QueryRouter::merge_plugin_result(Err(Error::UnsupportedStatement), &mut merged),
            Some(PluginOutput::Deny(_))
        ));
    }

    #[test]
    fn test_plugins_require_parse() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        // No plugins: nothing is required, even if the flag is set.
        qr.update_pool_settings(&PoolSettings {
            plugins_require_parse: true,
            ..Default::default()
        });
        assert!(!qr.plugins_require_parse());

        // Plugins from the global config on a pool with the parser off don't run.
        let mut settings = testing_plugin_settings(true);
        settings.query_parser_enabled = false;
        qr.update_pool_settings(&settings);
        assert!(!qr.plugins_enabled());
        assert!(!qr.plugins_require_parse());

        let mut settings = testing_plugin_settings(false);
        qr.update_pool_settings(&settings);
        assert!(!qr.plugins_require_parse());

        settings.plugins_require_parse = true;
        settings.query_parser_max_length = Some(48);
        if let Some(ref mut plugins) = settings.plugins {
            plugins.table_access = Some(crate::config::TableAccess {
                enabled: true,
                tables: vec!["secrets".into(), "a_very_long_table_name".into()],
            });
        }
        qr.update_pool_settings(&settings);
        assert!(qr.plugins_require_parse());

        // These might touch a table a plugin checks: rejected by the client
        // instead of being forwarded.
        for query in [
            "SELEKT * FROM secrets",
            "SELECT 1 FROM a_very_long_table_name, and_another_one",
            "INSERT INTO ONLY secrets VALUES (1)",
            "SELEKT * FROM U&\"\\0073ecrets\"",
        ] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(qr.rejects_unparsed(&message), "{}", query);
            assert!(
                qr.rejects_unparsed(&parse_message("", query, &[])),
                "{}",
                query
            );
        }

        // These can't: forwarded, for the server to answer.
        for query in ["SELEKT 1", "INSERT INTO ONLY other VALUES (1)"] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(!qr.rejects_unparsed(&message), "{}", query);
        }

        // RESET can't touch a table, parsed or not.
        let message = simple_query("RESET ALL");
        assert!(qr.parse(&message).is_ok());
        assert!(!qr.rejects_unparsed(&message));
        assert!(!qr.rejects_unparsed(&simple_query("reset search_path;")));
        assert!(!qr.rejects_unparsed(&parse_message("", "RESET ALL", &[])));
    }

    #[test]
    fn test_unparsed_vacuum_is_rejected_while_a_table_is_gated() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let with_gated = |tables: Vec<String>| {
            let mut settings = testing_plugin_settings(true);
            if let Some(ref mut plugins) = settings.plugins {
                plugins.table_access = Some(crate::config::TableAccess {
                    enabled: true,
                    tables,
                });
            }
            settings
        };

        // These process every table, so they might touch a gated one.
        let vacuums = [
            "VACUUM (FULL)",
            "vacuum (full, freeze)",
            "VACUUM FREEZE VERBOSE",
            "  -- comment\n\tVACUUM VERBOSE ANALYZE",
            "/* a /* nested */ comment */ VACUUM(FULL)",
            ";VACUUM (FULL)",
            "; ;VACUUM (FULL)",
            ";;VACUUM (FULL)",
            "/* c */;VACUUM (FULL)",
            "; -- c\nVACUUM (FULL)",
            "/* a */;/* b */VACUUM (FULL)",
            "\x0c;\x0bVACUUM (FULL)",
        ];
        qr.update_pool_settings(&with_gated(vec!["secrets".into()]));
        for query in vacuums {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(qr.rejects_unparsed(&message), "{}", query);
            assert!(
                qr.rejects_unparsed(&parse_message("", query, &[])),
                "{}",
                query
            );
        }
        for query in ["VACUUMS (FULL)", "/* VACUUM */ SELEKT 1"] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(!qr.rejects_unparsed(&message), "{}", query);
        }

        qr.update_pool_settings(&with_gated(vec![]));
        for query in vacuums {
            assert!(!qr.rejects_unparsed(&simple_query(query)), "{}", query);
        }
    }

    #[test]
    fn test_unparsed_string_bodies_are_rejected_while_a_table_is_gated() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let with_gated = |tables: Vec<String>| {
            let mut settings = testing_plugin_settings(true);
            if let Some(ref mut plugins) = settings.plugins {
                plugins.table_access = Some(crate::config::TableAccess {
                    enabled: true,
                    tables,
                });
            }
            settings
        };

        // The parser can't read these, and their bodies can name any table.
        let bodies = [
            "DO $$ BEGIN PERFORM 1; END $$",
            "do language plpgsql 'BEGIN PERFORM 1; END'",
            "/* c */ DO $x$ BEGIN PERFORM 1; END $x$",
            "SELECT 1; DO $$ BEGIN PERFORM 1; END $$",
            "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN PERFORM 1; END $$",
            "create or replace procedure p() language sql as 'SELECT 1'",
            "CREATE OR REPLACE FUNCTION f() RETURNS SETOF record LANGUAGE sql \
             AS $$ SELECT 1 $$ ROWS 10",
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql COST 1 AS $$ SELECT 1 $$",
            // Unterminated: the server reads up to the end.
            "DO $$ BEGIN PERFORM 1",
        ];
        qr.update_pool_settings(&with_gated(vec!["secrets".into()]));
        for query in bodies {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(qr.rejects_unparsed(&message), "{}", query);
            assert!(
                qr.rejects_unparsed(&parse_message("", query, &[])),
                "{}",
                query
            );
        }
        for query in [
            "SELEKT 'DO $$ $$'",
            "SELEKT 1; /* DO */ SELEKT 2",
            "SELEKT \"do\" $$ $$",
            "SELEKT 1 -- CREATE FUNCTION",
            // Text that can't be tokenized only might run one if it has
            // one of the words.
            "SELEKT 'unterminated",
        ] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(!qr.rejects_unparsed(&message), "{}", query);
        }
        for query in ["SELEKT 'Do", "SELEKT 1 $$ function"] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(qr.rejects_unparsed(&message), "{}", query);
        }

        // The tokenizer reads the lines after `COPY ... FROM STDIN;` as the
        // COPY's data; the server runs them as statements.
        let after_copy = [
            "COPY labels FROM STDIN;\nDO $$ BEGIN RAISE NOTICE 'hi'; END $$",
            "COPY labels FROM STDIN;\nCREATE FUNCTION f() RETURNS int LANGUAGE sql \
             AS $$ SELECT 1 $$",
            "COPY labels FROM STDIN;\nCREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$",
            "COPY labels FROM STDIN;\r\nDO $$ BEGIN PERFORM 1; END $$",
            "CREATE TEMP TABLE t (); COPY t FROM STDIN;\nDO $$ BEGIN PERFORM 1; END $$",
            "COPY labels FROM STDIN;\nCOPY other FROM STDIN;\nDO $$ BEGIN PERFORM 1; END $$",
            "COPY labels FROM STDIN;\nSELECT 1; COPY other FROM STDIN;\n\
             DO $$ BEGIN PERFORM 1; END $$",
            // A statement that starts on the `;` line and goes on in the data.
            "COPY labels FROM STDIN; CREATE\nFUNCTION f() RETURNS int LANGUAGE sql \
             AS $$ SELECT 1 $$",
            "COPY labels FROM STDIN; CREATE OR REPLACE\nPROCEDURE p() LANGUAGE sql \
             AS $$ SELECT 1 $$",
            // The server reads the `\.` line as part of the comment.
            "COPY labels FROM STDIN;\n/*\n\\.\nhello */ DO $$ BEGIN PERFORM 1; END $$",
        ];
        let deep = "COPY do_log FROM STDIN;\n".repeat(COPY_DATA_READS);
        let too_deep = "COPY do_log FROM STDIN;\n".repeat(COPY_DATA_READS + 1);
        for query in after_copy
            .into_iter()
            .chain([format!("{too_deep}SELECT 1").as_str()])
        {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{:?}", query);
            assert!(qr.rejects_unparsed(&message), "{:?}", query);
            assert!(
                qr.rejects_unparsed(&parse_message("", query, &[])),
                "{:?}",
                query
            );
        }
        for query in [
            "COPY do_log FROM STDIN;\nSELECT 1",
            "COPY do_log FROM STDIN;\nSELECT 'DO $$ $$'",
            "COPY do_log FROM STDIN;\nCOPY do_log FROM STDIN;\nSELECT 1",
            format!("{deep}SELECT 1").as_str(),
        ] {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{:?}", query);
            assert!(!qr.rejects_unparsed(&message), "{:?}", query);
        }

        qr.update_pool_settings(&with_gated(vec![]));
        for query in bodies.into_iter().chain(after_copy) {
            assert!(!qr.rejects_unparsed(&simple_query(query)), "{:?}", query);
        }

        // Without plugins_require_parse they're forwarded, even one whose
        // body names a gated table, like a string body the parser reads:
        // table access then checks names only.
        let mut settings = with_gated(vec!["secrets".into()]);
        settings.plugins_require_parse = false;
        qr.update_pool_settings(&settings);
        let names_it = "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql COST 1 \
                        AS $$ SELECT count(*) FROM secrets $$";
        for query in bodies.into_iter().chain(after_copy).chain([names_it]) {
            let message = simple_query(query);
            assert!(qr.parse(&message).is_err(), "{}", query);
            assert!(!qr.rejects_unparsed(&message), "{}", query);
        }
    }

    #[tokio::test]
    async fn test_parsed_string_bodies_are_refused_only_with_plugins_require_parse() {
        let mut qr = forbidding_secrets();
        let string_body = "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
                           AS $$ SELECT count(*) FROM secrets $$";
        let sql_body = "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
                        RETURN (SELECT count(*) FROM secrets)";
        let refusal =
            PluginOutput::Deny("permission for a function body given as a string denied".into());

        let ast = qr.parse(&simple_query(string_body)).unwrap();
        assert_eq!(qr.execute_plugins(&ast).await.unwrap(), refusal);

        // Without plugins_require_parse, with secrets still listed, table
        // access checks names only: the string body is sent, and the body
        // given as SQL is checked by the table it names.
        let mut settings = qr.pool_settings().clone();
        settings.plugins_require_parse = false;
        qr.update_pool_settings(&settings);
        assert!(!qr.plugins_require_parse());
        let ast = qr.parse(&simple_query(string_body)).unwrap();
        assert_eq!(qr.execute_plugins(&ast).await.unwrap(), PluginOutput::Allow);
        let ast = qr.parse(&simple_query(sql_body)).unwrap();
        assert_eq!(
            qr.execute_plugins(&ast).await.unwrap(),
            PluginOutput::Deny("permission for table \"secrets\" denied".into())
        );
    }

    #[tokio::test]
    async fn test_plugins_disabled_by_defaault() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        let query = simple_query("SELECT * FROM pg_database");
        let ast = qr.parse(&query).unwrap();

        let res = qr.execute_plugins(&ast).await;

        assert_eq!(res, Ok(PluginOutput::Allow));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_initializing_state() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        let query = simple_query("SELECT * FROM some_table");
        let ast = qr.parse(&query).unwrap();

        // Initially, the database activity should be in the "Initializing" state
        let state = qr.database_activity_state(&qr.pool_settings.db.clone());
        assert_eq!(state, DatabaseActivityState::Initializing);

        // Check that the router chooses the primary role due to "Initializing" state
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_active_state() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        let db_name = qr.pool_settings.db.clone();
        let cache = qr.database_activity_cache();
        cache.insert(db_name.clone(), DatabaseActivityState::Active);

        let query = simple_query("SELECT * FROM some_table");
        let ast = qr.parse(&query).unwrap();

        // Check that the router can choose a replica role when in "Active" state
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), None); // Default should allow replica due to active state
    }

    #[tokio::test]
    #[serial]
    async fn test_table_mutation_cache_on_write() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.table_mutation_cache_ms_ttl = 20_000; // 20 seconds in milliseconds
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        let query = simple_query("UPDATE some_table SET col1 = 'value' WHERE col2 = 1");
        let ast = qr.parse(&query).unwrap();

        // Simulate the mutation query which should populate the mutation cache
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));

        let table_cache_key = qr.table_mutation_cache_key(Ident::new("some_table"));
        let cache = qr.table_mutations_cache();

        // Ensure the table mutation cache contains the table with recent write
        assert!(cache.contains_key(&table_cache_key));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_multi_query() {
        use super::*;
        use crate::messages::simple_query;
        use tokio::time::Duration;

        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        // Configure the pool settings for db_activity_based_routing
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.db = "test_db_activity_routing".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        // First query when database is initializing
        let query = simple_query("SELECT * FROM test_table");
        let ast = qr.parse(&query).unwrap();
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because database is initializing
        assert_eq!(qr.role(), Some(Role::Primary));

        // Wait for the initialization delay to pass
        tokio::time::sleep(Duration::from_millis(
            qr.pool_settings.db_activity_init_delay * 2,
        ))
        .await;

        // Next query after database is active
        let query = simple_query("SELECT * FROM test_table");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to replica because database is active and no recent mutations
        assert_eq!(qr.role(), None);

        // Simulate a write query to update the mutation cache
        let query = simple_query("INSERT INTO test_table (id, name) VALUES (1, 'test')");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because it's a write operation
        assert_eq!(qr.role(), Some(Role::Primary));

        // Immediately run a read query on the same table
        let query = simple_query("SELECT * FROM test_table WHERE id = 1");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because the table was recently mutated
        assert_eq!(qr.role(), Some(Role::Primary));

        // Wait for the mutation cache TTL to expire
        tokio::time::sleep(Duration::from_millis(
            qr.pool_settings.table_mutation_cache_ms_ttl * 2,
        ))
        .await;

        // Run the read query again after cache expiration
        let query = simple_query("SELECT * FROM test_table WHERE id = 1");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to replica because mutation cache has expired
        assert_eq!(qr.role(), None);
    }
}
