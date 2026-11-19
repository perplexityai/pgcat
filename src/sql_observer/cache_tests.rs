//! `ClientObserver` with the prepared statement cache on: the statements in
//! the client's map of prepared statements are classified on the pool's cached
//! statement, or by the label the map keeps with them, and found through
//! pgcat's map (`StatementPlans`), driven as `client.rs` drives it.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};

use super::*;
use crate::messages::Parse;
use crate::plugins::statement_plans::{Cached, StatementPlans};
use crate::pool::PreparedStatementCache;
use crate::relations::Gate;

// --- Support ---

fn id(db: &str) -> PoolIdentifier {
    PoolIdentifier::new(db, "user")
}

/// One gate for these tests, as pgcat has one: epochs stay unique across pools.
static GATE: std::sync::Mutex<Gate> = std::sync::Mutex::new(Gate::new());

/// An enabled pool whose hub is published.
fn published(db: &str) -> SqlObserver {
    let pool = SqlObserver::new(true, &[], None, &id(db));
    GATE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .install(pool.hub());
    pool
}

fn absent() -> Caller<'static> {
    Caller::anonymous(IdentityState::Absent)
}

/// The client's Parse meta for `sql`, as `client.rs` builds it.
fn meta(pool: &SqlObserver, sql: &str) -> Snap {
    match sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql) {
        Ok(ast) => Snap::known(&ast, ClientObserver::new().classify_parse(pool, &ast)),
        Err(_) => Snap::Unparsed,
    }
}

fn statements(pool: &SqlObserver) -> u64 {
    pool.counters
        .identities
        .iter()
        .flatten()
        .map(|count| count.load(Ordering::Relaxed))
        .sum()
}

fn gap_total(pool: &SqlObserver, gap: Gap) -> u64 {
    pool.counters
        .gaps
        .iter()
        .map(|row| row[gap as usize].load(Ordering::Relaxed))
        .sum()
}

fn gaps(pool: &SqlObserver) -> u64 {
    Gap::ALL.iter().map(|gap| gap_total(pool, *gap)).sum()
}

fn rel(pool: &SqlObserver, label: &str) -> u64 {
    pool.hub()
        .samples()
        .into_iter()
        .find(|(name, _)| name == label)
        .map(|(_, v)| v.iter().sum())
        .unwrap_or_default()
}

/// A Parse frame's message, as the cache gets it.
fn parse_message(name: &str, query: &str) -> Parse {
    let mut buf = BytesMut::new();
    let name = format!("{}\0", name);
    let query = format!("{}\0", query);
    buf.put_u8(b'P');
    buf.put_i32((4 + name.len() + query.len() + 2) as i32);
    buf.put_slice(name.as_bytes());
    buf.put_slice(query.as_bytes());
    buf.put_i16(0);
    Parse::try_from(&buf).unwrap()
}

/// A client with the cache on, as `client.rs` drives its observer: the map
/// (pgcat's `StatementPlans`) takes a Parse when it is buffered, with the
/// observer's label for it, drops a statement (the unnamed one too) when its
/// Close is buffered, and puts back what a refused batch changed.
struct CachedClient {
    /// The client's map of statements.
    pub map: StatementPlans,
    pub observer: ClientObserver,
    /// Statements and portals pgcat answers itself.
    pub local: Vec<String>,
}

impl CachedClient {
    fn new() -> CachedClient {
        CachedClient {
            map: StatementPlans::default(),
            observer: ClientObserver::cached(),
            local: Vec::new(),
        }
    }

    /// `buffer_parse`: `cached` is the SQL after plugins, `meta` the
    /// classification of what the client wrote.
    fn parse(
        &mut self,
        cache: &mut PreparedStatementCache,
        name: &str,
        meta: Snap,
        cached: &str,
        rewritten: bool,
    ) {
        self.local.retain(|local| local != name);
        let parse = parse_message(name, cached);
        let hash = parse.get_hash();
        let shared = cache.get_or_insert(&parse, hash);
        let label =
            self.observer
                .stage_cached_parse(name.as_bytes(), meta, shared.observed(), rewritten);
        self.map.cache(
            name.to_string(),
            Some(Cached {
                parse: shared,
                hash,
                rewritten,
                label: label.map(Box::new),
            }),
        );
    }

    /// `buffer_parse` while the pool's observer is off: no label.
    fn parse_unobserved(
        &mut self,
        cache: &mut PreparedStatementCache,
        name: &str,
        cached: &str,
        rewritten: bool,
    ) {
        let parse = parse_message(name, cached);
        let hash = parse.get_hash();
        let shared = cache.get_or_insert(&parse, hash);
        self.map.cache(
            name.to_string(),
            Some(Cached {
                parse: shared,
                hash,
                rewritten,
                label: None,
            }),
        );
    }

    /// `buffer_local_parse`.
    fn local_parse(&mut self, name: &str, meta: Snap) {
        self.map.cache(name.to_string(), None);
        if !self.local.iter().any(|local| local == name) {
            self.local.push(name.to_string());
        }
        self.observer.stage_parse(name.as_bytes(), meta);
    }

    /// The cached statement the map holds for `name`.
    fn held(&self, name: &str) -> Arc<Parse> {
        self.map.cached(name).expect("held").parse.clone()
    }

    fn holds(&self, name: &str) -> bool {
        self.map.cached(name).is_some()
    }

    fn rewritten(&self, name: &str) -> bool {
        self.map.cached(name).is_some_and(|cached| cached.rewritten)
    }

    /// The observer's label the map keeps for `name`.
    fn label(&self, name: &str) -> Option<Snap> {
        self.map.cached(name)?.label.as_deref().copied()
    }

    /// `replan_stale_statement`, on a Bind of `name` after a reload changed
    /// what a plugin covers: the client's own Parse, planned again, is now cached
    /// as `cached`, `rewritten` if a plugin now changes it.
    fn replan(
        &mut self,
        cache: &mut PreparedStatementCache,
        name: &str,
        cached: &str,
        rewritten: bool,
    ) {
        let parse = parse_message(name, cached);
        let hash = parse.get_hash();
        let shared = cache.get_or_insert(&parse, hash);
        let replanned = Cached::replanned(self.map.cached(name), shared, hash, rewritten);
        self.map.cache(name.to_string(), Some(replanned));
    }

    /// `buffer_bind`: false if pgcat refuses it (the statement isn't in the
    /// map), which errors the client.
    fn bind(&mut self, portal: &str, name: &str) -> bool {
        let key = BindKey::new(portal.as_bytes(), name.as_bytes());
        if self.local.iter().any(|local| local == name) {
            self.observer.stage_bind_key(&key);
            return true;
        }
        match self.map.cached(name) {
            Some(cached) => {
                self.observer.stage_cached_bind(
                    &key,
                    cached.parse.observed(),
                    cached.rewritten,
                    cached.label.as_deref().copied(),
                );
                true
            }
            None => false,
        }
    }

    /// `buffer_close` of a statement: the map drops the name, with its
    /// label, until the batch is sent or refused.
    fn close_statement(&mut self, name: &str) {
        self.map.close(name.to_string());
        let local = match self.local.iter().position(|local| local == name) {
            Some(index) => {
                self.local.remove(index);
                true
            }
            None => false,
        };
        self.observer
            .stage_cached_close(name.as_bytes(), false, local);
    }

    /// The buffered batch is sent: its changes stand.
    fn send(&mut self) {
        self.map.commit();
        self.observer.send_batch();
    }

    /// The buffered batch is answered by pgcat alone: its changes stand, as
    /// when it is sent.
    fn accept_local(&mut self) {
        self.map.commit();
        self.observer.accept_local_batch();
    }

    /// The buffered batch is refused: the map puts back what it changed,
    /// labels included (`reset_buffered_state`).
    fn refuse(&mut self) {
        self.map.rollback();
        self.observer.reject_batch();
    }
}

fn cache(size: usize) -> PreparedStatementCache {
    PreparedStatementCache::new(size)
}

// --- Focused tests ---

#[test]
fn a_named_statement_is_found_through_the_clients_map() {
    let pool = published("lookup");
    let mut cache = cache(16);
    let sql = "SELECT * FROM x.a";
    let a = meta(&pool, sql);

    let mut first = CachedClient::new();
    first.parse(&mut cache, "s", a, sql, false);
    // Classified on the cached statement; nothing kept per client.
    assert_eq!(first.held("s").observed().get(), Some(a));
    assert_eq!(first.observer.live().0, 0);
    assert!(first.bind("p", "s"));
    first.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((statements(&pool), rel(&pool, "a")), (1, 1));

    // Another client preparing the same SQL under another name holds the same
    // cached statement and its classification.
    let mut second = CachedClient::new();
    second.parse(&mut cache, "other", a, sql, false);
    assert!(Arc::ptr_eq(&first.held("s"), &second.held("other")));
    assert_eq!(second.observer.live().0, 0);
    assert!(second.bind("", "other"));
    second.observer.admit_execute(Some(b""), absent(), &pool);
    assert_eq!((statements(&pool), rel(&pool, "a"), gaps(&pool)), (2, 2, 0));
}

#[test]
fn more_than_128_statements_have_no_gaps() {
    let pool = published("many");
    let mut cache = cache(2_000);
    let mut client = CachedClient::new();
    // Under the hub's 1,000 relation names.
    let count = 4 * SLOT_CAP;
    for i in 0..count {
        let sql = format!("SELECT * FROM x.t{}", i);
        client.parse(
            &mut cache,
            &format!("s{}", i),
            meta(&pool, &sql),
            &sql,
            false,
        );
    }
    client.send();
    client.observer.server_replied(false, Some(b'T'));
    for i in 0..count {
        let name = format!("s{}", i);
        assert!(client.bind("p", &name));
        client.observer.admit_execute(Some(b"p"), absent(), &pool);
    }
    assert!(!client.observer.quarantined);
    assert_eq!(client.observer.live(), (0, 1));
    assert_eq!((statements(&pool), gaps(&pool)), (count as u64, 0));
    assert_eq!(rel(&pool, "t500"), 1);
}

#[test]
fn with_the_cache_off_the_tables_are_a0s() {
    let observer = ClientObserver::new();
    assert!(!observer.cached);
    assert_eq!(
        (observer.statements.len(), observer.portals.len()),
        (SLOT_CAP, SLOT_CAP)
    );
}

// A refused batch puts back what it changed in the client's map
// (`reset_buffered_state`), so after [Close s1, Parse s1=B, <refused>, Sync]
// s1 still runs A, and the observer, which follows the map, counts A. (With
// the cache off, the observer's refused redefinition stands: it counts B.)
#[test]
fn a_refused_redefinition_is_counted_as_what_the_map_runs() {
    let pool = published("refused");
    let (sql_a, sql_b) = ("SELECT * FROM x.a", "SELECT * FROM x.b");
    let mut cache = cache(16);
    let mut client = CachedClient::new();

    client.parse(&mut cache, "s1", meta(&pool, sql_a), sql_a, false);
    client.send();
    client.observer.server_replied(false, Some(b'I'));

    // [Close s1, Parse s1=B, <refused>, Sync]
    client.close_statement("s1");
    client.parse(&mut cache, "s1", meta(&pool, sql_b), sql_b, false);
    client.refuse();
    assert_eq!(client.held("s1").query(), sql_a);

    assert!(client.bind("p", "s1"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "a"), rel(&pool, "b"), gaps(&pool)), (1, 0, 0));

    // [Close s1, <refused>, Sync]: the map dropped s1 when the Close was
    // buffered and puts it back, so s1 still runs and the cached observer
    // counts it. (With the cache off, the observer
    // forgets a name at its Close, refused or not: a gap.)
    client.close_statement("s1");
    client.refuse();
    assert!(client.holds("s1"));
    assert!(client.bind("p2", "s1"));
    client.observer.admit_execute(Some(b"p2"), absent(), &pool);
    assert_eq!((rel(&pool, "a"), gaps(&pool)), (2, 0));

    // Sent, the Close drops s1 from the map: a Bind of it is refused by pgcat.
    client.close_statement("s1");
    client.send();
    assert!(!client.bind("p3", "s1"));
}

// A statement whose shared classification isn't the client's (the
// unnamed one, one a plugin rewrote, one the client classified otherwise)
// has its label in the client's map, so a refused redefinition puts the old
// label back with the old statement: counted as A, the SQL that runs.
#[test]
fn a_refused_redefinition_puts_the_label_back() {
    let pool = published("refused-label");
    let (sql_a, sql_b) = ("SELECT * FROM x.a", "SELECT * FROM x.b");
    let mut cache = cache(16);
    // (name, B as the client writes it, B as the cache gets it, rewritten)
    let cases = [
        ("", sql_b, sql_b, false),
        ("rw", sql_b, "SELECT * FROM x.theirs", true),
        ("odd", sql_b, sql_b, false),
    ];
    for (i, (name, written, cached, rewritten)) in cases.into_iter().enumerate() {
        let mut client = CachedClient::new();
        client.parse(&mut cache, name, meta(&pool, sql_a), sql_a, false);
        client.send();
        client.observer.server_replied(false, Some(b'I'));

        let b = if name == "odd" {
            // Classified otherwise than the shared one: e.g. not parsed.
            Snap::Unparsed
        } else {
            meta(&pool, written)
        };
        if !name.is_empty() {
            client.close_statement(name);
        }
        client.parse(&mut cache, name, b, cached, rewritten);
        assert_eq!(client.label(name), Some(b));
        client.refuse();
        assert_eq!(client.held(name).query(), sql_a);
        assert!(!client.rewritten(name));

        assert!(client.bind("p", name));
        client.observer.admit_execute(Some(b"p"), absent(), &pool);
        let counted = (i + 1) as u64;
        assert_eq!(
            (
                rel(&pool, "a"),
                rel(&pool, "b"),
                rel(&pool, "theirs"),
                gaps(&pool)
            ),
            (counted, 0, 0, 0),
            "{:?}",
            name
        );
        // Nothing kept per client.
        assert_eq!(client.observer.live().0, 0);
    }
}

// A batch the server fails is sent: what it changed in the map stands, so a
// redefinition the server skipped is counted as B, which pgcat runs at the
// next Bind (the statement is prepared again under B's cached name).
#[test]
fn a_failed_redefinition_is_counted_as_what_the_map_runs() {
    let pool = published("failed-label");
    let (sql_a, sql_b) = ("SELECT * FROM x.a", "SELECT * FROM x.b");
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "", meta(&pool, sql_a), sql_a, false);
    client.send();
    client.observer.server_replied(false, Some(b'I'));

    client.parse(&mut cache, "", meta(&pool, sql_b), sql_b, false);
    client.send();
    client.observer.server_replied(true, Some(b'I'));
    assert_eq!(client.held("").query(), sql_b);
    assert!(client.bind("p", ""));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "a"), rel(&pool, "b")), (0, 1));
}

#[test]
fn an_evicted_statement_keeps_its_classification() {
    let pool = published("evict");
    let (sql_a, sql_b) = ("SELECT * FROM x.a", "SELECT * FROM x.b");
    let mut cache = cache(1);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "a", meta(&pool, sql_a), sql_a, false);
    let held = client.held("a").clone();
    // B evicts A from the pool's cache; the client still holds A.
    client.parse(&mut cache, "b", meta(&pool, sql_b), sql_b, false);
    assert!(client.bind("p", "a"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "a"), rel(&pool, "b")), (1, 0));

    // Parsed again, A is a new cached statement, classified anew; the
    // evicted one keeps its classification for those who hold it.
    let mut other = CachedClient::new();
    other.parse(&mut cache, "a", meta(&pool, sql_a), sql_a, false);
    assert!(!Arc::ptr_eq(&held, &other.held("a")));
    assert_eq!(other.held("a").observed().get(), held.observed().get());
    assert!(other.bind("p", "a"));
    other.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert!(client.bind("p", "a"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "a"), rel(&pool, "b"), gaps(&pool)), (3, 0, 0));

    // A statement from a cache whose hub is gone counts as unknown, not as
    // another relation.
    let pool2 = published("evict-other-hub");
    assert!(client.bind("p", "a"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool2);
    assert_eq!((rel(&pool2, "a"), rel(&pool2, "unknown")), (0, 1));
}

#[test]
fn a_rewritten_statement_is_never_counted_as_the_cached_sql() {
    let pool = published("rewrite");
    let (original, cached) = ("SELECT * FROM x.mine", "SELECT * FROM x.theirs");
    let mut cache = cache(16);

    // Another client prepared the cached SQL as it wrote it.
    let mut other = CachedClient::new();
    other.parse(&mut cache, "s", meta(&pool, cached), cached, false);

    // A plugin rewrote this client's SQL into the same cached statement.
    let mut client = CachedClient::new();
    client.parse(&mut cache, "s", meta(&pool, original), cached, true);
    assert!(Arc::ptr_eq(&client.held("s"), &other.held("s")));
    assert!(client.rewritten("s"));
    assert_eq!(client.observer.live().0, 0);
    assert_eq!(client.label("s"), Some(meta(&pool, original)));
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "mine"), rel(&pool, "theirs")), (1, 0));

    // Its label stays through what the map keeps: a lifecycle clear, or an
    // overflow of the observer's own tables.
    client.observer.clear(true, true);
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "mine"), rel(&pool, "theirs")), (2, 0));
    client.observer.overflow();
    client.observer.accept_local_batch();
    client.observer.idle();
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "mine"), rel(&pool, "theirs")), (3, 0));
    assert_eq!(gaps(&pool), 0);
    // Parsed again as the client wrote it, the map marks it unrewritten and
    // it is the shared statement's.
    client.parse(&mut cache, "s", meta(&pool, cached), cached, false);
    assert!(!client.rewritten("s"));
    assert_eq!(client.label("s"), None);
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!(rel(&pool, "theirs"), 1);
}

// D-2: whether a statement was rewritten, and its label, are
// the map's, per statement, so an overflow of the observer's own tables loses
// neither.
#[test]
fn an_overflow_loses_no_label() {
    let pool = published("rewrite-lost");
    let (original, cached, plain) = (
        "SELECT * FROM x.mine",
        "SELECT * FROM x.theirs",
        "SELECT * FROM x.plain",
    );
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "r", meta(&pool, original), cached, true);
    client.parse(&mut cache, "s", meta(&pool, plain), plain, false);
    client.send();
    client.observer.server_replied(false, Some(b'I'));

    client.observer.overflow();
    client.observer.accept_local_batch();
    client.observer.idle();
    assert!(!client.observer.quarantined);
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "plain"), gaps(&pool)), (1, 0));
    assert!(client.bind("p", "r"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((rel(&pool, "mine"), rel(&pool, "theirs")), (1, 0));
    assert_eq!(gaps(&pool), 0);
}

#[test]
fn a_client_classifying_otherwise_keeps_its_own() {
    let pool = published("mismatch");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut first = CachedClient::new();
    first.parse(&mut cache, "s", meta(&pool, sql), sql, false);
    // The same text classified otherwise (e.g. by the parser's length limit,
    // which counts the statement's name): kept per client, never shared.
    let mut second = CachedClient::new();
    second.parse(&mut cache, "s", Snap::Unparsed, sql, false);
    assert_eq!(second.label("s"), Some(Snap::Unparsed));
    assert!(second.bind("p", "s"));
    second.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!(
        (rel(&pool, "a"), gap_total(&pool, Gap::UnparsedStatement)),
        (0, 1)
    );
    // Parsed as the shared one again, the label goes.
    second.parse(&mut cache, "s", meta(&pool, sql), sql, false);
    assert_eq!(second.label("s"), None);
}

// D-1: only a parsed classification is shared, so a client that didn't parse
// the SQL (here, as when a long statement name puts the Parse over
// `query_parser_max_length`) doesn't leave every later client a gap.
#[test]
fn an_unparsed_first_offer_leaves_the_shared_classification_open() {
    let pool = published("unparsed-first");
    let sql = "SELECT * FROM x.a";
    let a = meta(&pool, sql);
    let mut cache = cache(16);
    let mut long = CachedClient::new();
    long.parse(&mut cache, "s", Snap::Unparsed, sql, false);
    assert_eq!(long.held("s").observed().get(), None);
    assert_eq!(long.label("s"), Some(Snap::Unparsed));

    let mut short = CachedClient::new();
    short.parse(&mut cache, "s", a, sql, false);
    assert!(Arc::ptr_eq(&long.held("s"), &short.held("s")));
    assert_eq!(short.held("s").observed().get(), Some(a));
    assert_eq!(short.label("s"), None);
    assert!(short.bind("p", "s"));
    short.observer.admit_execute(Some(b"p"), absent(), &pool);
    // The client that didn't parse it still has its own gap.
    assert!(long.bind("p", "s"));
    long.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!(
        (rel(&pool, "a"), gap_total(&pool, Gap::UnparsedStatement)),
        (1, 1)
    );
}

#[test]
fn statements_held_before_the_observer_are_unknown_if_rewritten() {
    let pool = published("late");
    let (cached, plain) = ("SELECT * FROM x.theirs", "SELECT * FROM x.plain");
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    // "s" is classified in the shared cache (by any client); "r" was
    // rewritten and parsed before the observer was on: no label.
    client.parse(&mut cache, "s", meta(&pool, plain), plain, false);
    client.parse_unobserved(&mut cache, "r", cached, true);
    // The observer is created later (the pool's was enabled by a reload).
    client.observer = ClientObserver::cached();

    // No rewrite: the statement has the client's own SQL, so the shared
    // classification is its own.
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!(rel(&pool, "plain"), 1);
    // A rewrite: unknown until Parsed again, as with A0.
    assert!(client.bind("p", "r"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!(
        (
            rel(&pool, "theirs"),
            gap_total(&pool, Gap::UnparsedStatement)
        ),
        (0, 1)
    );
}

// The unnamed statement is in the client's map as the named ones are, and
// pgcat runs it under its cached name as long as the map holds it: after a
// simple Query, which destroys the server's unnamed statement. So it is
// counted, with its label, as A0 never was. Its Close, which pgcat forwards,
// drops it from the map when buffered, as a named one's does; a refused batch
// puts it back. A statement the client parsed before the observer was on has
// no label: a gap.
#[test]
fn the_unnamed_statement_is_counted_while_the_map_runs_it() {
    let pool = published("unnamed");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "", meta(&pool, sql), sql, false);
    assert_eq!(client.observer.live().0, 0);
    assert_eq!(client.label(""), Some(meta(&pool, sql)));
    client
        .observer
        .admit_query(QueryAdmission::Parsed(Invalidate::Nothing));
    assert!(client.bind("", ""));
    client.observer.admit_execute(Some(b""), absent(), &pool);
    assert_eq!((rel(&pool, "a"), gaps(&pool)), (1, 0));

    client.send();

    // [Close "", <refused>, Sync]: the map puts it back, so it still runs.
    client.close_statement("");
    assert!(!client.holds(""));
    client.refuse();
    assert!(client.holds(""));
    assert!(client.bind("", ""));
    client.observer.admit_execute(Some(b""), absent(), &pool);
    assert_eq!((rel(&pool, "a"), gaps(&pool)), (2, 0));
    client.send();

    // Sent, the Close drops it: a Bind of it is refused by pgcat.
    client.close_statement("");
    client.send();
    assert!(!client.holds(""));
    assert!(!client.bind("", ""));

    client.parse_unobserved(&mut cache, "", sql, false);
    assert!(client.bind("", ""));
    client.observer.admit_execute(Some(b""), absent(), &pool);
    assert_eq!(
        (rel(&pool, "a"), gap_total(&pool, Gap::UnparsedStatement)),
        (2, 1)
    );
}

#[test]
fn statements_pgcat_answers_are_kept_per_client() {
    let pool = published("local");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "s", meta(&pool, sql), sql, false);
    // Intercepted by a plugin: answered by pgcat, out of the map.
    client.local_parse("s", Snap::Excluded);
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((statements(&pool), gaps(&pool)), (0, 0));
    // Closed, it is gone at once, as in A0.
    client.close_statement("s");
    assert_eq!(client.observer.live().0, 0);
    assert!(!client.bind("p", "s"));

    // [Parse s, Close s, <s answered by pgcat>]: the Close dropped s from the
    // map when it was buffered, before the local Parse, so the observer keeps
    // the statement pgcat answers.
    client.parse(&mut cache, "s", meta(&pool, sql), sql, false);
    client.close_statement("s");
    client.local_parse("s", Snap::Excluded);
    client.send();
    assert!(client.bind("p", "s"));
    client.observer.admit_execute(Some(b"p"), absent(), &pool);
    assert_eq!((statements(&pool), gaps(&pool)), (0, 0));
}

// F-A0-1: a portal generation executed in a refused batch stays executed, so
// retrying its Execute counts nothing; a fresh Bind counts again, as in A0.
#[test]
fn a_refused_batch_keeps_its_executed_portal_as_in_a0() {
    let pool = published("f-a0-1");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "A", meta(&pool, sql), sql, false);
    assert!(client.bind("P", "A"));
    client.accept_local();

    client.observer.admit_execute(Some(b"P"), absent(), &pool);
    client.refuse();
    assert_eq!(statements(&pool), 1);
    client.observer.admit_execute(Some(b"P"), absent(), &pool);
    assert_eq!(statements(&pool), 1);
    assert!(client.bind("P", "A"));
    client.observer.admit_execute(Some(b"P"), absent(), &pool);
    assert_eq!(statements(&pool), 2);
}

// F-A0-2: portals don't outlive the server: released, the observer drops
// them, and their Executes are gaps, as in A0.
#[test]
fn portals_are_dropped_with_the_server_as_in_a0() {
    let pool = published("f-a0-2");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    client.parse(&mut cache, "A", meta(&pool, sql), sql, false);
    assert!(client.bind("P", "A"));
    client.send();
    client.observer.server_released();
    assert_eq!(client.observer.live(), (0, 0));
    client.observer.admit_execute(Some(b"P"), absent(), &pool);
    assert_eq!(
        (
            statements(&pool),
            gap_total(&pool, Gap::MetadataInvalidated)
        ),
        (0, 1)
    );
    // The statement stays in the map: a fresh Bind counts.
    assert!(client.bind("P", "A"));
    client.observer.admit_execute(Some(b"P"), absent(), &pool);
    assert_eq!(statements(&pool), 1);
}

// A statement planned again after a reload changed what a plugin covers
// (`replan_stale_statement`) keeps what the observer knew of the client's
// SQL, which is the same: a label stands, and a statement rewritten before or
// now is never counted as the cached SQL.
#[test]
fn a_replanned_statement_keeps_its_label_and_rewritten_mark() {
    let pool = published("replan");
    let (mine, theirs) = ("SELECT * FROM x.mine", "SELECT * FROM x.theirs");
    let mut cache = cache(16);
    // Another client prepared both SQL texts as it wrote them, so both have
    // a shared classification a mislabel would count.
    let mut other = CachedClient::new();
    other.parse(&mut cache, "m", meta(&pool, mine), mine, false);
    other.parse(&mut cache, "t", meta(&pool, theirs), theirs, false);
    let mut client = CachedClient::new();
    let count = |client: &mut CachedClient, name: &str| {
        assert!(client.bind("p", name));
        client.observer.admit_execute(Some(b"p"), absent(), &pool);
        (rel(&pool, "mine"), rel(&pool, "theirs"), gaps(&pool))
    };

    // Unrewritten and unlabelled, it is rewritten now: a gap, not the cached
    // SQL's classification.
    client.parse(&mut cache, "s", meta(&pool, mine), mine, false);
    assert_eq!((client.rewritten("s"), client.label("s")), (false, None));
    client.replan(&mut cache, "s", theirs, true);
    assert_eq!((client.rewritten("s"), client.label("s")), (true, None));
    assert_eq!(count(&mut client, "s"), (0, 0, 1));

    // Rewritten while the observer was off (no label), it isn't now: still a
    // gap, as nothing knows what the client classified.
    client.parse_unobserved(&mut cache, "r", theirs, true);
    client.replan(&mut cache, "r", mine, false);
    assert_eq!((client.rewritten("r"), client.label("r")), (true, None));
    assert_eq!(count(&mut client, "r"), (0, 0, 2));

    // Labelled (the unnamed statement), it is rewritten now: its label still
    // counts it.
    client.parse(&mut cache, "", meta(&pool, mine), mine, false);
    client.replan(&mut cache, "", theirs, true);
    assert_eq!(client.label(""), Some(meta(&pool, mine)));
    assert_eq!(count(&mut client, ""), (1, 0, 2));

    // The replan is put back with the batch pgcat refuses.
    client.send();
    client.replan(&mut cache, "s", mine, false);
    assert!(Arc::ptr_eq(&client.held("s"), &other.held("m")));
    client.refuse();
    assert!(Arc::ptr_eq(&client.held("s"), &other.held("t")));
}

#[test]
fn layout() {
    let statement = std::mem::size_of::<Option<StatementEntry>>();
    let portal = std::mem::size_of::<Option<PortalEntry>>();
    let header = std::mem::size_of::<ClientObserver>();
    let off = ClientObserver::new();
    assert_eq!(
        off.retained_bytes(),
        header + SLOT_CAP * statement + SLOT_CAP * portal
    );

    let pool = published("layout");
    let sql = "SELECT * FROM x.a";
    let mut cache = cache(16);
    let mut client = CachedClient::new();
    let fresh = client.observer.retained_bytes();
    assert_eq!(fresh, header);
    for i in 0..4 * SLOT_CAP {
        client.parse(&mut cache, &format!("s{}", i), meta(&pool, sql), sql, false);
    }
    assert_eq!(client.observer.retained_bytes(), fresh);
    // So is the unnamed statement: its label is in the map.
    client.parse(&mut cache, "", meta(&pool, sql), sql, false);
    let unnamed = client.observer.retained_bytes();
    assert_eq!(unnamed, fresh);
    // The first portal takes a table of the smallest capacity; up to 4
    // distinct ones fit.
    for portal_name in ["", "p1", "p2", "p3"] {
        assert!(client.bind(portal_name, "s0"));
    }
    let bound = client.observer.retained_bytes();
    assert_eq!(bound, unnamed + 4 * portal);
    // A statement pgcat answers takes a statement table of the smallest
    // capacity.
    client.local_parse("l", Snap::Excluded);
    let local = client.observer.retained_bytes();
    assert_eq!(local, bound + 4 * statement);
    // Tables keep their capacity when emptied.
    client.observer.server_released();
    client.observer.idle();
    assert_eq!(client.observer.retained_bytes(), local);
    // At most, A0's tables.
    for i in 0..SLOT_CAP {
        assert!(client.bind(&format!("q{}", i), "s0"));
    }
    assert!(!client.observer.quarantined);
    assert_eq!(client.observer.live().1, SLOT_CAP);
    assert!(client.observer.retained_bytes() <= header + SLOT_CAP * (statement + portal));
    assert!(client.bind("one more", "s0"));
    assert!(client.observer.quarantined);

    let shared = std::mem::size_of::<SharedSnap>();
    assert!(shared <= 56, "{}", shared);
    // The map's entry per statement (each hash bucket), with the observer's
    // boxed label: was the renamed Parse's Arc and its hash, 16 B.
    // 32 B on x86_64; the 44 B `Snap` is allocated only for a labelled one.
    let cached = std::mem::size_of::<Cached>();
    assert!(cached <= 32, "{}", cached);
    println!(
        "layout: header {} B, statement entry {} B, portal entry {} B; \
         cache off {} B; cache on fresh {} B, with the unnamed statement {} B, \
         and up to 4 portals {} B, at most {} B; SharedSnap {} B, Parse {} B, \
         Cached {} B, Snap {} B",
        header,
        statement,
        portal,
        off.retained_bytes(),
        fresh,
        unnamed,
        bound,
        header + SLOT_CAP * statement + SLOT_CAP * portal,
        shared,
        std::mem::size_of::<Parse>(),
        cached,
        std::mem::size_of::<Snap>()
    );
}
