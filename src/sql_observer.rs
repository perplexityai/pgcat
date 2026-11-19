//! Counts the SQL statements clients send through a pool for execution.
//!
//! A statement is counted once, when pgcat admits it, as the client wrote it:
//! before plugins rewrite or refuse it, whether or not the server then runs it.
//! What pgcat doesn't count (simple queries it doesn't parse, and Executes whose
//! statement it has no metadata for) is reported as a gap event instead. A gap
//! is not a statement count: it says the count is incomplete, not by how much.
//!
//! Extended protocol: Parse, Bind and Describe count nothing. The first
//! Execute staged for a Bind (a portal generation) counts the statement as the
//! client Parsed it, once; repeated Executes of the same generation (a cursor
//! being fetched) count nothing. The bookkeeping lives in `ClientObserver`,
//! with fixed capacities: when it can't keep track it forgets, and the Executes
//! it no longer has metadata for become gap events.
//!
//! With the prepared statement cache on, a named statement's classification
//! lives on the pool's cached statement (`SharedSnap`), found through the
//! client's own map of the statements it prepared, so the observer keeps no
//! per-client entry for it and has no statement cap: it counts what the
//! client's map makes run. See `ClientObserver::cached`.
//!
//! Commands pgcat answers itself (`PGCAT_IDENTITY`, its custom commands) and
//! the SQL pgcat sends on its own aren't counted.
//!
//! Labels are pool and user names from the config, and the client's
//! `application_name` folded to the pool's configured list
//! (`sql_observer_clients`, else `other`); never tenants or tokens.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use log::warn;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::identity_label::IdentityState;
use crate::pool::{get_all_pools, PoolIdentifier};
use crate::relations::{self, Caller, RelationHub, RelationSnap, GATE};

/// Why statements went uncounted. In `Gap::ALL` order, which indexes the counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gap {
    /// A simple Query the parser didn't parse (or isn't enabled to), forwarded.
    UnparsedQuery,
    /// A simple Query refused before it was parsed.
    RefusedUnparsed,
    /// An Execute of a prepared statement the parser didn't parse, or of one
    /// dropped before the server was last idle: the reason for a loss doesn't
    /// outlive an idle, so this reads as statement metadata unavailable.
    UnparsedStatement,
    /// An Execute of a portal pgcat has no record of (e.g. a SQL cursor's name).
    UnknownPortal,
    /// An Execute whose metadata was dropped: a backend error, a statement that
    /// ended the portal, the observer's bounds, or a statement it never saw.
    MetadataInvalidated,
}

impl Gap {
    const ALL: [Gap; 5] = [
        Gap::UnparsedQuery,
        Gap::RefusedUnparsed,
        Gap::UnparsedStatement,
        Gap::UnknownPortal,
        Gap::MetadataInvalidated,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Gap::UnparsedQuery => "unparsed_query",
            Gap::RefusedUnparsed => "refused_unparsed",
            Gap::UnparsedStatement => "unparsed_statement",
            Gap::UnknownPortal => "unknown_portal",
            Gap::MetadataInvalidated => "metadata_invalidated",
        }
    }
}

/// At most this many `sql_observer_clients` per pool. With `other`, the
/// `client` label takes at most `MAX_CLIENTS + 1` values per pool and user.
pub const MAX_CLIENTS: usize = 32;
/// A configured client name is at most this many bytes.
pub const MAX_CLIENT_NAME_BYTES: usize = 64;

/// Column 0 is `other`; column `n` is the `n`th client name of the lineage.
const COLUMNS: usize = MAX_CLIENTS + 1;
const OTHER_CLIENT: usize = 0;
const OTHER_LABEL: &str = "other";

/// Why `sql_observer_clients` is invalid, if it is.
pub fn invalid_clients(clients: &[String]) -> Option<String> {
    if clients.len() > MAX_CLIENTS {
        return Some(format!(
            "at most {} names, got {}",
            MAX_CLIENTS,
            clients.len()
        ));
    }
    for (i, name) in clients.iter().enumerate() {
        if name.is_empty() || name.len() > MAX_CLIENT_NAME_BYTES {
            return Some(format!(
                "names must be 1 to {} bytes, got {:?}",
                MAX_CLIENT_NAME_BYTES, name
            ));
        }
        if name.chars().any(char::is_control) {
            return Some(format!(
                "names can't contain control characters, got {:?}",
                name
            ));
        }
        // `SHOW CONFIG` lists the names joined by commas.
        if name.contains(',') {
            return Some(format!("names can't contain commas, got {:?}", name));
        }
        if name == OTHER_LABEL {
            return Some(format!(
                "{:?} is the label of every other name",
                OTHER_LABEL
            ));
        }
        if clients[..i].contains(name) {
            return Some(format!("{:?} is listed twice", name));
        }
    }
    None
}

#[derive(Debug)]
struct Counters {
    /// Statements, by client column and the client's `IdentityState`.
    identities: [[AtomicU64; IdentityState::COUNT]; COLUMNS],
    /// By client column and `Gap`, in `Gap::ALL` order.
    gaps: [[AtomicU64; Gap::ALL.len()]; COLUMNS],
    /// The client names of the lineage, column `n` at `n - 1`: append-only,
    /// so a name keeps its column (and counts) across reloads, even after it
    /// is dropped from the config.
    clients: Mutex<Vec<Box<str>>>,
    /// The relation slots and their counters, see `relations`.
    relations: RelationHub,
}

impl Counters {
    fn new(pool: &str, user: &str) -> Counters {
        Counters {
            identities: std::array::from_fn(|_| Default::default()),
            gaps: std::array::from_fn(|_| Default::default()),
            clients: Mutex::new(Vec::new()),
            relations: RelationHub::new(pool, user),
        }
    }
}

/// The counters of every `SqlObserver::default()`, which is disabled and
/// counts nothing: settings built per client connection, then replaced by the
/// pool's, would otherwise each allocate a full table.
static PLACEHOLDER: Lazy<Arc<Counters>> = Lazy::new(|| Arc::new(Counters::new("", "")));

/// A pool's statement counts. Disabled, it counts nothing.
#[derive(Clone, Debug)]
pub struct SqlObserver {
    enabled: bool,
    /// The pool's configured client names and their columns.
    clients: Arc<[(Box<str>, u8)]>,
    /// Shared with the pool this one replaced on reload, so the counts stay
    /// monotonic and clients still on the old pool aren't lost.
    counters: Arc<Counters>,
}

impl Default for SqlObserver {
    fn default() -> SqlObserver {
        SqlObserver {
            enabled: false,
            clients: Arc::from([]),
            counters: PLACEHOLDER.clone(),
        }
    }
}

impl SqlObserver {
    /// `clients` are the `application_name`s counted under their own `client`
    /// label (validated by `invalid_clients`); `id` names the pool in the
    /// observer's logs.
    pub fn new(
        enabled: bool,
        clients: &[String],
        previous: Option<&SqlObserver>,
        id: &PoolIdentifier,
    ) -> SqlObserver {
        let counters = match previous {
            Some(previous) => previous.counters.clone(),
            None => Arc::new(Counters::new(&id.db, &id.user)),
        };
        let clients = {
            let mut lineage = counters.clients.lock();
            clients
                .iter()
                .filter_map(|name| {
                    let column = match lineage.iter().position(|known| **known == **name) {
                        Some(at) => at + 1,
                        None if lineage.len() < MAX_CLIENTS => {
                            lineage.push(name.as_str().into());
                            lineage.len()
                        }
                        None => {
                            warn!(
                                "[pool: {}][user: {}] SQL observer client names are full ({} since the pool was added); {:?} counts as other",
                                id.db, id.user, MAX_CLIENTS, name
                            );
                            return None;
                        }
                    };
                    Some((name.as_str().into(), column as u8))
                })
                .collect()
        };
        SqlObserver {
            enabled,
            clients,
            counters,
        }
    }

    /// The `client` column of an `application_name`.
    fn client_column(&self, application_name: &str) -> usize {
        self.clients
            .iter()
            .find(|(name, _)| **name == *application_name)
            .map_or(OTHER_CLIENT, |(_, column)| *column as usize)
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// `statements` admitted: parsed from one simple Query, or the statement of
    /// the first Execute of a portal.
    pub fn attempts(&self, statements: usize, caller: Caller<'_>) {
        if !self.enabled {
            return;
        }
        self.counters.identities[self.client_column(caller.application_name)]
            [caller.identity.column()]
        .fetch_add(statements as u64, Ordering::Relaxed);
    }

    pub fn gap(&self, gap: Gap, caller: Caller<'_>) {
        if !self.enabled {
            return;
        }
        self.counters.gaps[self.client_column(caller.application_name)][gap as usize]
            .fetch_add(1, Ordering::Relaxed);
    }

    /// The relation slots shared along this pool's reload lineage.
    pub fn hub(&self) -> &RelationHub {
        &self.counters.relations
    }

    /// A statement was admitted (and counted by `attempts`): count its relations,
    /// or `unknown` if the hub it was classified against is gone.
    pub fn relation_attempt(&self, snap: RelationSnap, caller: Caller<'_>) {
        if !self.enabled {
            return;
        }
        self.counters.relations.admit(snap, caller);
    }
}

/// Install the candidate pools' relation hubs (a published one keeps its
/// slots), retire the published hubs nothing uses any more, and publish the
/// candidate pools, all while holding the publication gate. `store` publishes
/// and must not await.
pub fn publish(candidates: &[&SqlObserver], store: impl FnOnce()) {
    let mut gate = GATE.lock();
    let current = get_all_pools();
    let current: Vec<&RelationHub> = current
        .values()
        .map(|pool| pool.settings.sql_observer.hub())
        .collect();
    let candidates: Vec<&RelationHub> = candidates.iter().map(|observer| observer.hub()).collect();
    gate.publish(&current, &candidates);
    store();
}

// ---------------------------------------------------------------------------
// Per-client bookkeeping for the extended protocol.
// ---------------------------------------------------------------------------

/// The longest name the observer keeps, in bytes (without the NUL).
pub const NAME_CAP: usize = 128;
/// Statement slots and portal slots, each.
pub const SLOT_CAP: usize = 128;

/// What a statement's first Execute makes the observer forget, from its
/// original AST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalidate {
    Nothing,
    /// COMMIT, ROLLBACK (also TO SAVEPOINT, AND CHAIN) and CLOSE.
    Portals,
    /// DEALLOCATE and DISCARD.
    Both,
}

/// The effect of the original AST of an admitted statement.
pub fn invalidation(ast: &[sqlparser::ast::Statement]) -> Invalidate {
    use sqlparser::ast::Statement;

    let mut effect = Invalidate::Nothing;
    for statement in ast {
        match statement {
            Statement::Deallocate { .. } | Statement::Discard { .. } => return Invalidate::Both,
            Statement::Commit { .. } | Statement::Rollback { .. } | Statement::Close { .. } => {
                effect = Invalidate::Portals
            }
            _ => (),
        }
    }
    effect
}

/// What a prepared statement (or the portal bound to it) counts and drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Snap {
    /// Not SQL to count: pgcat's identity commands.
    Excluded,
    /// Parsed: `count` is 0 (empty query) or 1 statement.
    Known {
        count: u8,
        invalidate: Invalidate,
        relations: RelationSnap,
    },
    /// pgcat didn't parse it.
    Unparsed,
    /// Bound to a statement the observer had lost.
    Invalidated,
}

impl Snap {
    /// From the original AST of a Parse, before plugins rewrite it.
    pub fn known(ast: &[sqlparser::ast::Statement], relations: RelationSnap) -> Snap {
        Snap::Known {
            count: u8::from(!ast.is_empty()),
            invalidate: invalidation(ast),
            relations,
        }
    }
}

/// The observer's classification of a cached prepared statement's SQL, kept
/// on the statement in the pool's prepared statement cache and shared by every
/// client holding it.
///
/// Set once, by the first client that stages the statement as it wrote it (not
/// rewritten by a plugin) and parsed it (`Snap::Known`), so it is the
/// classification of the cached SQL text under the pool's parser and observer
/// config, never of a client or tenant. A client that didn't parse it (e.g. a
/// name long enough to put the Parse over `query_parser_max_length`) leaves it
/// for the next. A client staging the same statement compares its own
/// classification with it and has its own label kept with the statement in
/// its map if they differ (`ClientObserver::stage_cached_parse`). A
/// plugin-rewritten statement is never counted with it. A clone starts empty,
/// as the copy can be given other SQL.
#[derive(Debug, Default)]
pub struct SharedSnap(std::sync::OnceLock<Snap>);

impl Clone for SharedSnap {
    fn clone(&self) -> SharedSnap {
        SharedSnap::default()
    }
}

impl SharedSnap {
    /// Offer `meta` as the shared classification: whether it is the shared one.
    /// Only a parsed classification is shared.
    fn offer(&self, meta: Snap) -> bool {
        if !matches!(meta, Snap::Known { .. }) {
            return false;
        }
        *self.0.get_or_init(|| meta) == meta
    }

    fn get(&self) -> Option<Snap> {
        self.0.get().copied()
    }
}

/// How a simple Query was admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryAdmission {
    Parsed(Invalidate),
    Unparsed,
}

/// A statement or portal name, kept as a 128-bit keyed hash instead of its
/// bytes: two SipHash outputs under two process-random keys, so a client can't
/// aim for a collision. A collision would only make the observer take one name
/// for another, which mislabels telemetry (counts and gaps) and never reaches
/// what pgcat forwards. Names longer than `NAME_CAP` are still refused, as when
/// names were kept inline, so which names the observer keeps is unchanged.
/// The halves stay two `u64`s: a `u128`'s 16-byte alignment pads the entries.
#[derive(Clone, Copy, PartialEq, Eq)]
struct NameKey([u64; 2]);

impl NameKey {
    /// The unnamed statement or portal: no other name maps to it.
    const UNNAMED: NameKey = NameKey([0; 2]);

    fn new(name: &[u8]) -> Option<NameKey> {
        use std::hash::{BuildHasher, Hasher};

        if name.len() > NAME_CAP {
            return None;
        }
        if name.is_empty() {
            return Some(NameKey::UNNAMED);
        }
        static KEYS: std::sync::OnceLock<[std::collections::hash_map::RandomState; 2]> =
            std::sync::OnceLock::new();
        let keys = KEYS.get_or_init(Default::default);
        let half = |state: &std::collections::hash_map::RandomState| {
            let mut hasher = state.build_hasher();
            hasher.write(name);
            hasher.finish()
        };
        let key = [half(&keys[0]), half(&keys[1])];
        if key == NameKey::UNNAMED.0 {
            return Some(NameKey([0, 1]));
        }
        Some(NameKey(key))
    }
}

#[derive(Clone, Copy)]
struct PortalValue {
    snap: Snap,
    executed: bool,
}

#[derive(Clone, Copy)]
struct StatementEntry {
    name: NameKey,
    meta: Snap,
}

#[derive(Clone, Copy)]
struct PortalEntry {
    name: NameKey,
    value: PortalValue,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Statement,
    Portal,
}

// The observer's retained state, at most: see `ClientObserver`. The classifier
// scratch is per worker thread (`relations::classify`), not per client.
const _: () = {
    assert!(std::mem::size_of::<Snap>() <= 48);
    assert!(std::mem::size_of::<Option<StatementEntry>>() <= 64);
    assert!(std::mem::size_of::<Option<PortalEntry>>() <= 64);
    assert!(std::mem::size_of::<ClientObserver>() <= 128);
    assert!(
        SLOT_CAP * 64 + SLOT_CAP * 64 + 128 <= 16_512,
        "the observer's state exceeds its budget"
    );
};

/// What one client's extended protocol messages let the observer count.
///
/// Statements by the name the client gave them, and portals by theirs, in two
/// tables of at most `SLOT_CAP` entries (allocated up front with the cache
/// off, growing as used with it on): the observer forgets rather than grows
/// past them (see `overflow`), which loses telemetry only.
///
/// What a batch stages stands once staged: nothing is taken back when pgcat
/// refuses the batch or the server answers it with an error (see
/// `reject_batch` and `server_replied`).
///
/// With the prepared statement cache on (`cached`), the statements in the
/// client's map of prepared statements are looked up through it instead, and
/// the statement table only keeps those pgcat answers itself (see `cached`).
pub struct ClientObserver {
    /// Cache off: all statements, `SLOT_CAP` slots allocated up front. Cache
    /// on: only the statements pgcat answers itself, growing to `SLOT_CAP`.
    statements: Box<[Option<StatementEntry>]>,
    /// Cache off: `SLOT_CAP` slots allocated up front. Cache on: growing to
    /// `SLOT_CAP` as the client binds distinct portals (the slots stay).
    portals: Box<[Option<PortalEntry>]>,
    /// The client's prepared statement cache is on.
    cached: bool,
    /// Something was forgotten since the last idle: why a lookup came up empty.
    invalidated: bool,
    /// The observer lost track: no metadata is kept until the dispatches
    /// already sent have been answered and the batch being staged is over.
    quarantined: bool,
    /// Dispatches sent whose ReadyForQuery hasn't come back.
    unanswered: u32,
    unanswered_overflowed: bool,
    /// A batch is being staged.
    staging: bool,
}

/// `len` empty slots. Built with `vec!` so a release build writes only each
/// slot's None tag, as A0's did: a push loop there copied whole slots.
fn table<T: Copy>(len: usize) -> Box<[Option<T>]> {
    vec![None; len].into_boxed_slice()
}

/// A full table grows with the cache on, doubling from 4 slots up to
/// `SLOT_CAP`: its first new slot, or None if it is at `SLOT_CAP` already. A
/// table allocated up front (the cache off) is at `SLOT_CAP` already, so there
/// this is only reached where A0 overflows. Out of line and taking only the
/// table, so the paths that find a slot compile as A0's.
#[cold]
#[inline(never)]
fn grow<T: Copy>(table: &mut Box<[Option<T>]>) -> Option<usize> {
    let len = table.len();
    if len >= SLOT_CAP {
        return None;
    }
    let mut slots = Vec::with_capacity((len * 2).clamp(4, SLOT_CAP));
    slots.extend_from_slice(table);
    slots.resize(slots.capacity(), None);
    *table = slots.into_boxed_slice();
    Some(len)
}

impl Default for ClientObserver {
    fn default() -> ClientObserver {
        ClientObserver::new()
    }
}

impl ClientObserver {
    /// A client whose prepared statement cache is off.
    pub fn new() -> ClientObserver {
        ClientObserver {
            statements: table(SLOT_CAP),
            portals: table(SLOT_CAP),
            cached: false,
            invalidated: false,
            quarantined: false,
            unanswered: 0,
            unanswered_overflowed: false,
            staging: false,
        }
    }

    /// A client whose prepared statement cache is on.
    ///
    /// A statement in the client's map, the unnamed one included, is
    /// classified on the cached statement the map holds, or by the label the
    /// map keeps with it where the shared classification isn't the client's
    /// (`stage_cached_parse`, `stage_cached_bind`). So it is counted as what
    /// the map makes it run, for as long as it does: a Close takes effect when
    /// the map drops the name, a batch pgcat drops puts the label back with
    /// the statement, and nothing the observer would otherwise forget
    /// (lifecycle statements, its bounds) drops it. The map marks the
    /// statements a plugin rewrote on their way to the cache: those are only
    /// counted with their own label. The statements pgcat answers itself and
    /// the portals are kept per client as with the cache off, in tables that
    /// grow to `SLOT_CAP` as they are used instead of being allocated up
    /// front.
    pub fn cached() -> ClientObserver {
        ClientObserver {
            statements: Box::new([]),
            portals: Box::new([]),
            cached: true,
            invalidated: false,
            quarantined: false,
            unanswered: 0,
            unanswered_overflowed: false,
            staging: false,
        }
    }

    /// The relations of an original statement, as the pool's hub's slots.
    /// The classifier scratch is the worker thread's, not the client's.
    pub fn classify(
        &mut self,
        pool: &SqlObserver,
        statement: &sqlparser::ast::Statement,
    ) -> RelationSnap {
        relations::classify(pool.hub(), statement)
    }

    /// The relations of a Parse's original AST: one statement is classified,
    /// an empty query has none, several are unknown.
    pub fn classify_parse(
        &mut self,
        pool: &SqlObserver,
        ast: &[sqlparser::ast::Statement],
    ) -> RelationSnap {
        match ast {
            [statement] => self.classify(pool, statement),
            _ => RelationSnap::UNKNOWN,
        }
    }

    // --- Lookup ---

    fn statement_index(&self, name: &NameKey) -> Option<usize> {
        self.statements
            .iter()
            .position(|slot| matches!(slot, Some(entry) if entry.name == *name))
    }

    fn portal_index(&self, name: &NameKey) -> Option<usize> {
        self.portals
            .iter()
            .position(|slot| matches!(slot, Some(entry) if entry.name == *name))
    }

    // --- Bounds ---

    /// Forget everything: the observer couldn't keep track. Nothing but
    /// telemetry is lost; the Executes that needed what's forgotten become gaps.
    fn overflow(&mut self) {
        self.statements.iter_mut().for_each(|slot| *slot = None);
        self.portals.iter_mut().for_each(|slot| *slot = None);
        self.invalidated = true;
        self.quarantined = true;
    }

    fn maybe_recover(&mut self) {
        if self.quarantined && self.unanswered == 0 && !self.unanswered_overflowed && !self.staging
        {
            self.quarantined = false;
        }
    }

    // --- Staging, in the batch being built ---

    /// A Parse was staged.
    pub fn stage_parse(&mut self, name: &[u8], meta: Snap) {
        self.staging = true;
        if self.quarantined {
            return;
        }
        let Some(name) = NameKey::new(name) else {
            return self.overflow();
        };
        let index = self.statement_index(&name);
        let slot = match index.or_else(|| self.statements.iter().position(Option::is_none)) {
            Some(slot) => slot,
            None => match grow(&mut self.statements) {
                Some(slot) => slot,
                None => return self.overflow(),
            },
        };
        self.statements[slot] = Some(StatementEntry { name, meta });
    }

    /// A Parse was staged that the client's map of prepared statements now
    /// holds as `shared`'s statement from the pool's cache: `rewritten` if a
    /// plugin changed its SQL text on the way. Returns the label the map keeps
    /// with the statement for `stage_cached_bind`: the client's own
    /// classification, or None if the shared one is it. Kept in the map, the
    /// label is put back with the statement when pgcat drops the batch, and
    /// goes when the map drops the statement.
    #[must_use]
    pub fn stage_cached_parse(
        &mut self,
        name: &[u8],
        meta: Snap,
        shared: &SharedSnap,
        rewritten: bool,
    ) -> Option<Snap> {
        self.staging = true;
        // A statement pgcat answered under the name is replaced.
        if let Some(index) = NameKey::new(name).and_then(|name| self.statement_index(&name)) {
            self.statements[index] = None;
        }
        if !name.is_empty() && !rewritten && shared.offer(meta) {
            return None;
        }
        // The unnamed statement, another client's SQL text cached for it, or
        // its own classified otherwise (or not parsed): its own label.
        Some(meta)
    }

    /// A Bind was staged: the portal snapshots what the statement is now.
    pub fn stage_bind(&mut self, portal: &[u8], statement: &[u8]) {
        self.stage_bind_key(&BindKey::new(portal, statement));
    }

    /// As `stage_bind`, for names read from the client's frame before it was
    /// rewritten.
    pub fn stage_bind_key(&mut self, key: &BindKey) {
        self.staging = true;
        if self.quarantined {
            return;
        }
        let Some(portal) = key.portal else {
            return self.overflow();
        };
        let found = key
            .statement
            .and_then(|statement| self.statement_index(&statement));
        let snap = match found {
            Some(index) => self.statements[index].map_or(Snap::Unparsed, |entry| entry.meta),
            None if self.invalidated => Snap::Invalidated,
            None => Snap::Unparsed,
        };
        self.bind_portal(portal, snap);
    }

    /// A Bind was staged of a statement the client's map holds as `shared`'s
    /// statement from the pool's cache (`key` as in `stage_bind_key`), with
    /// the `rewritten` mark and the `label` the map keeps with it (see
    /// `stage_cached_parse`).
    pub fn stage_cached_bind(
        &mut self,
        key: &BindKey,
        shared: &SharedSnap,
        rewritten: bool,
        label: Option<Snap>,
    ) {
        self.staging = true;
        if self.quarantined {
            return;
        }
        let Some(portal) = key.portal else {
            return self.overflow();
        };
        let snap = match label {
            Some(snap) => snap,
            // Parsed before the observer was on. Its cached SQL may be
            // another client's: only its own label counts it.
            None if rewritten || key.statement == Some(NameKey::UNNAMED) => self.missing(),
            None => shared.get().unwrap_or_else(|| self.missing()),
        };
        self.bind_portal(portal, snap);
    }

    /// What a Bind of a statement the observer has no record of snapshots.
    fn missing(&self) -> Snap {
        if self.invalidated {
            Snap::Invalidated
        } else {
            Snap::Unparsed
        }
    }

    #[inline(always)]
    fn bind_portal(&mut self, portal: NameKey, snap: Snap) {
        let index = self.portal_index(&portal);
        let slot = match index.or_else(|| self.portals.iter().position(Option::is_none)) {
            Some(slot) => slot,
            None => match grow(&mut self.portals) {
                Some(slot) => slot,
                None => return self.overflow(),
            },
        };
        self.portals[slot] = Some(PortalEntry {
            name: portal,
            value: PortalValue {
                snap,
                executed: false,
            },
        });
    }

    /// A protocol Close of a statement (`portal` false) or portal was staged.
    pub fn stage_close(&mut self, name: &[u8], portal: bool) {
        self.staging = true;
        if self.quarantined {
            return;
        }
        // Nothing this long is kept, so nothing to drop.
        let Some(name) = NameKey::new(name) else {
            return;
        };
        self.invalidated |= self.forget_name(
            if portal {
                Namespace::Portal
            } else {
                Namespace::Statement
            },
            &name,
        );
    }

    /// As `stage_close`, with the cache on: `local` if pgcat answers the
    /// statement itself. A statement in the client's map has its label there.
    /// Buffering its Close removes the entry and label together, including the
    /// unnamed statement; a locally refused batch restores both through the
    /// map's journal. The unnamed Close is still forwarded.
    pub fn stage_cached_close(&mut self, name: &[u8], portal: bool, local: bool) {
        if portal || local {
            return self.stage_close(name, portal);
        }
        self.staging = true;
    }

    /// An Execute was staged (`portal` is None if its name is too long to be
    /// kept, or malformed): count it, or say why it can't be.
    pub fn admit_execute(&mut self, portal: Option<&[u8]>, caller: Caller<'_>, pool: &SqlObserver) {
        self.staging = true;
        let found = portal
            .and_then(NameKey::new)
            .and_then(|name| self.portal_index(&name));
        let Some(index) = found else {
            let gap = if self.invalidated {
                Gap::MetadataInvalidated
            } else {
                Gap::UnknownPortal
            };
            return pool.gap(gap, caller);
        };
        let entry = self.portals[index].expect("indexed portal");
        match entry.value.snap {
            Snap::Excluded => (),
            Snap::Unparsed => {
                pool.gap(Gap::UnparsedStatement, caller);
                self.clear(true, true);
            }
            Snap::Invalidated => {
                pool.gap(Gap::MetadataInvalidated, caller);
                self.clear(true, true);
            }
            Snap::Known {
                count,
                invalidate,
                relations,
            } => {
                if entry.value.executed {
                    return;
                }
                if let Some(live) = self.portals[index].as_mut() {
                    live.value.executed = true;
                }
                pool.attempts(count as usize, caller);
                if count > 0 {
                    pool.relation_attempt(relations, caller);
                }
                match invalidate {
                    Invalidate::Nothing => (),
                    Invalidate::Portals => self.clear(true, false),
                    Invalidate::Both => self.clear(true, true),
                }
            }
        }
    }

    // --- Admission-time invalidation ---

    /// Forget the portals and/or statements.
    fn clear(&mut self, portals: bool, statements: bool) {
        let mut changed = false;
        if portals {
            changed |= self.portals.iter().any(Option::is_some);
            self.portals.iter_mut().for_each(|slot| *slot = None);
        }
        if statements {
            if self.cached {
                // The statements in the client's map stay there and still run.
                changed |= self.forget_name(Namespace::Statement, &NameKey::UNNAMED);
            } else {
                changed |= self.statements.iter().any(Option::is_some);
                self.statements.iter_mut().for_each(|slot| *slot = None);
            }
        }
        self.invalidated |= changed;
    }

    /// Forget what a name in a namespace stands for: whether it stood for
    /// anything.
    fn forget_name(&mut self, namespace: Namespace, name: &NameKey) -> bool {
        match namespace {
            Namespace::Statement => match self.statement_index(name) {
                Some(index) => self.statements[index].take().is_some(),
                None => false,
            },
            Namespace::Portal => match self.portal_index(name) {
                Some(index) => self.portals[index].take().is_some(),
                None => false,
            },
        }
    }

    /// A simple Query was admitted.
    pub fn admit_query(&mut self, admission: QueryAdmission) {
        // Every simple Query destroys the unnamed statement and portal.
        for namespace in [Namespace::Statement, Namespace::Portal] {
            self.invalidated |= self.forget_name(namespace, &NameKey::UNNAMED);
        }
        match admission {
            QueryAdmission::Parsed(Invalidate::Nothing) => (),
            QueryAdmission::Parsed(Invalidate::Portals) => self.clear(true, false),
            QueryAdmission::Parsed(Invalidate::Both) | QueryAdmission::Unparsed => {
                self.clear(true, true)
            }
        }
    }

    // --- Ending a batch ---

    /// The batch being staged is refused. Nothing the observer keeps is taken
    /// back: what it staged stands, as in `accept_local_batch`, and its counts
    /// stay. So with the cache off a name whose redefinition was refused is
    /// attributed to the refused definition until it is defined again, and a
    /// portal it executed stays executed. With the cache on, the labels of
    /// the statements in the client's map are the map's, and go back with it
    /// (see `stage_cached_parse`). Telemetry only; the server never saw the
    /// batch.
    pub fn reject_batch(&mut self) {
        self.accept_local_batch();
    }

    /// The batch being staged is answered by pgcat alone: what it changed
    /// stands.
    pub fn accept_local_batch(&mut self) {
        self.staging = false;
        self.maybe_recover();
    }

    fn sent(&mut self) {
        match self.unanswered.checked_add(1) {
            Some(unanswered) => self.unanswered = unanswered,
            None => self.unanswered_overflowed = true,
        }
    }

    /// The batch being staged goes to the server.
    pub fn send_batch(&mut self) {
        self.staging = false;
        self.sent();
    }

    /// A simple Query goes to the server, ahead of the batch being staged.
    pub fn send_query(&mut self) {
        self.sent();
    }

    // --- Server replies ---

    /// What the server answered to a dispatch of the client: whether it
    /// contained an ErrorResponse, and the ReadyForQuery status ending it.
    ///
    /// An ErrorResponse forgets nothing: the names a failed dispatch Parsed,
    /// Bound or Closed keep what the observer staged for them, though the
    /// server may not have done it. So a name created in a failed batch counts
    /// as an attempt when it is executed, and a name whose redefinition failed
    /// is attributed to the failed definition until it is defined again.
    /// That is why `_error` is intentionally ignored.
    pub fn server_replied(&mut self, _error: bool, ready_for_query: Option<u8>) {
        if self.unanswered == 0 && !self.unanswered_overflowed {
            return;
        }
        let Some(status) = ready_for_query else {
            return;
        };
        if !self.unanswered_overflowed {
            self.unanswered -= 1;
        }
        if status == b'I' {
            self.idle();
        }
        self.maybe_recover();
    }

    /// The server is idle: portals don't outlive the transaction.
    pub fn idle(&mut self) {
        self.forget_portals();
        // The reason doesn't outlive an actual idle, quarantine or not: what is
        // asked for after it is just unknown.
        self.invalidated = false;
    }

    /// Portals the observer holds are dropped; the reason bit stays.
    fn forget_portals(&mut self) -> bool {
        let had = self.portals.iter().any(Option::is_some);
        self.portals.iter_mut().for_each(|slot| *slot = None);
        had
    }

    /// The server is taken from the client: its portals are gone.
    pub fn server_released(&mut self) {
        if self.forget_portals() {
            self.invalidated = true;
        }
    }

    // --- Names in the messages ---

    #[cfg(test)]
    fn statement_meta(&self, name: &[u8]) -> Option<Snap> {
        self.statement_index(&NameKey::new(name)?)
            .and_then(|index| self.statements[index].map(|entry| entry.meta))
    }

    #[cfg(test)]
    fn portal_value(&self, name: &[u8]) -> Option<(Snap, bool)> {
        self.portal_index(&NameKey::new(name)?).and_then(|index| {
            self.portals[index].map(|entry| (entry.value.snap, entry.value.executed))
        })
    }

    /// What the observer keeps for the client: itself and its tables' heap.
    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<ClientObserver>()
            + self.statements.len() * std::mem::size_of::<Option<StatementEntry>>()
            + self.portals.len() * std::mem::size_of::<Option<PortalEntry>>()
    }

    #[cfg(test)]
    fn live(&self) -> (usize, usize) {
        (
            self.statements.iter().flatten().count(),
            self.portals.iter().flatten().count(),
        )
    }
}

/// The names of a Bind, copied out of the client's frame (it is renamed
/// afterwards). A name too long to be kept is None.
#[derive(Clone, Copy)]
pub struct BindKey {
    portal: Option<NameKey>,
    statement: Option<NameKey>,
}

impl BindKey {
    pub fn new(portal: &[u8], statement: &[u8]) -> BindKey {
        BindKey {
            portal: NameKey::new(portal),
            statement: NameKey::new(statement),
        }
    }

    pub fn from_frame(message: &[u8]) -> Option<BindKey> {
        bind_names(message).map(|(portal, statement)| BindKey::new(portal, statement))
    }
}

/// The NUL-terminated name starting at `at` in a frontend message, without
/// the NUL and without copying it.
fn name_at(message: &[u8], at: usize) -> Option<&[u8]> {
    let rest = message.get(at..)?;
    let end = rest.iter().position(|byte| *byte == 0)?;
    Some(&rest[..end])
}

/// Parse (P): the statement name.
pub fn parse_name(message: &[u8]) -> Option<&[u8]> {
    name_at(message, 5)
}

/// Execute (E): the portal name.
pub fn execute_portal(message: &[u8]) -> Option<&[u8]> {
    name_at(message, 5)
}

/// Bind (B): the portal name and the statement name.
pub fn bind_names(message: &[u8]) -> Option<(&[u8], &[u8])> {
    let portal = name_at(message, 5)?;
    let statement = name_at(message, 5 + portal.len() + 1)?;
    Some((portal, statement))
}

/// Close (C): whether it is a portal's, and the name.
pub fn close_target(message: &[u8]) -> Option<(bool, &[u8])> {
    let portal = match message.get(5)? {
        b'P' => true,
        b'S' => false,
        _ => return None,
    };
    Some((portal, name_at(message, 6)?))
}

const OBSERVING: &str = "pgcat_sql_observer_observing";
const ATTEMPTS: &str = "pgcat_sql_observer_attempts_total";
const GAPS: &str = "pgcat_sql_observer_gap_events_total";
const RELATION_ATTEMPTS: &str = "pgcat_sql_observer_relation_attempts_total";

/// The metrics in the Prometheus text format, for every pool.
pub fn prometheus_lines() -> Vec<String> {
    // The gate keeps a reload from publishing between the pool snapshot and
    // the registry reads, so labels and values come from one registry.
    let gate = GATE.lock();
    let pools = get_all_pools();
    let mut observers: Vec<_> = pools
        .iter()
        .map(|(id, pool)| (id, &pool.settings.sql_observer))
        .collect();
    observers.sort_by(|a, b| (&a.0.db, &a.0.user).cmp(&(&b.0.db, &b.0.user)));
    let samples: Vec<_> = observers
        .iter()
        .filter(|(_, observer)| observer.enabled)
        .map(|(id, observer)| (*id, observer.hub().samples()))
        .collect();
    drop(gate);
    let mut lines = render(&observers);
    render_relations(&mut lines, &samples);
    lines
}

type RelationSamples = Vec<(String, [u64; IdentityState::COUNT])>;

fn render_relations(lines: &mut Vec<String>, pools: &[(&PoolIdentifier, RelationSamples)]) {
    header(
        lines,
        RELATION_ATTEMPTS,
        "Relations named by admitted SQL statements, by bare table name (the first 1000 distinct per pool and user; later ones count as other) plus other, unknown and no_explicit_relation, and by the client's identity (present, absent, expired, refused); a statement naming several relations counts in each, so not a statement count",
        "counter",
    );
    for (id, samples) in pools {
        for (relation, values) in samples {
            for (identity, value) in IdentityState::ALL.iter().map(|s| s.as_str()).zip(values) {
                lines.push(format!(
                    "{}{{{},relation=\"{}\",identity=\"{}\"}} {}",
                    RELATION_ATTEMPTS,
                    pool_labels(id),
                    label(relation),
                    identity,
                    value
                ));
            }
        }
    }
}

/// A pool that isn't observing reports so, and no counts: zero would read as
/// no traffic.
fn render(observers: &[(&PoolIdentifier, &SqlObserver)]) -> Vec<String> {
    let mut lines = Vec::new();

    header(
        &mut lines,
        OBSERVING,
        "1 if the pool counts the SQL statements clients send, 0 if not",
        "gauge",
    );
    for (id, observer) in observers {
        lines.push(format!(
            "{}{{{}}} {}",
            OBSERVING,
            pool_labels(id),
            u8::from(observer.enabled)
        ));
    }

    header(
        &mut lines,
        ATTEMPTS,
        "SQL statements admitted from parsed simple queries and the first Execute of extended portals, by the client's identity: present (verified and unexpired), absent (never set, or reset), expired, or refused (the last SET PGCAT_IDENTITY was); a lower bound, see gap events",
        "counter",
    );
    for (id, observer) in observers.iter().filter(|(_, observer)| observer.enabled) {
        for (client, column) in exported_clients(&observer.counters) {
            let counters = &observer.counters.identities[column];
            for (identity, counter) in IdentityState::ALL.iter().zip(counters) {
                lines.push(format!(
                    "{}{{{},client=\"{}\",identity=\"{}\"}} {}",
                    ATTEMPTS,
                    pool_labels(id),
                    client,
                    identity.as_str(),
                    counter.load(Ordering::Relaxed)
                ));
            }
        }
    }

    header(
        &mut lines,
        GAPS,
        "Messages whose SQL statements weren't counted, by reason; not a statement count",
        "counter",
    );
    for (id, observer) in observers.iter().filter(|(_, observer)| observer.enabled) {
        for (client, column) in exported_clients(&observer.counters) {
            let counters = &observer.counters.gaps[column];
            for (gap, counter) in Gap::ALL.iter().zip(counters) {
                lines.push(format!(
                    "{}{{{},client=\"{}\",reason=\"{}\"}} {}",
                    GAPS,
                    pool_labels(id),
                    client,
                    gap.as_str(),
                    counter.load(Ordering::Relaxed)
                ));
            }
        }
    }

    lines
}

/// The clients a pool exports, in both families, as their escaped labels and
/// columns: `other` always, so an observing pool exports its series before
/// any traffic, and a configured client once it has a nonzero count in either
/// family, so idle pools don't export every client's zeros. A client never
/// disappears once exported, as its counts only grow.
fn exported_clients(counters: &Counters) -> Vec<(String, usize)> {
    let names = counters.clients.lock();
    std::iter::once((OTHER_LABEL.to_string(), OTHER_CLIENT))
        .chain(names.iter().enumerate().filter_map(|(at, name)| {
            let column = at + 1;
            counters.identities[column]
                .iter()
                .chain(&counters.gaps[column])
                .any(|count| count.load(Ordering::Relaxed) > 0)
                .then(|| (label(name), column))
        }))
        .collect()
}

fn pool_labels(id: &PoolIdentifier) -> String {
    format!("pool=\"{}\",user=\"{}\"", label(&id.db), label(&id.user))
}

fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn header(lines: &mut Vec<String>, name: &str, help: &str, kind: &str) {
    lines.push(format!("# HELP {} {}", name, help));
    lines.push(format!("# TYPE {} {}", name, kind));
}

#[cfg(test)]
mod cache_tests;

#[cfg(test)]
mod tests {

    // --- ClientObserver ---

    const ONE: Snap = Snap::Known {
        count: 1,
        invalidate: Invalidate::Nothing,
        relations: RelationSnap::UNKNOWN,
    };

    fn pool() -> SqlObserver {
        SqlObserver::new(true, &[], None, &id("db"))
    }

    fn gap_total(pool: &SqlObserver, gap: Gap) -> u64 {
        pool.counters
            .gaps
            .iter()
            .map(|row| row[gap as usize].load(Ordering::Relaxed))
            .sum()
    }

    fn statements(pool: &SqlObserver) -> u64 {
        pool.counters
            .identities
            .iter()
            .flatten()
            .map(|count| count.load(Ordering::Relaxed))
            .sum()
    }

    /// Parse and Bind a statement, as one batch the server accepted.
    fn bound(observer: &mut ClientObserver, statement: &[u8], portal: &[u8], meta: Snap) {
        observer.stage_parse(statement, meta);
        observer.stage_bind(portal, statement);
    }

    #[test]
    fn first_execute_of_a_portal_counts_once() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        bound(&mut observer, b"s", b"p", ONE);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(statements(&pool), 1);
        // Bound again: a new generation counts again.
        observer.stage_bind(b"p", b"s");
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(statements(&pool), 2);
    }

    #[test]
    fn unknown_unparsed_and_invalidated_executes_are_gaps() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        observer.admit_execute(Some(b"cursor"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::UnknownPortal), 1);

        bound(&mut observer, b"s", b"p", Snap::Unparsed);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::UnparsedStatement), 1);

        // That cleared everything, so what is asked for now is invalidated.
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);
        assert_eq!(statements(&pool), 0);
    }

    #[test]
    fn committing_statement_drops_portals_after_it_is_counted() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        let commit = Snap::Known {
            count: 1,
            invalidate: Invalidate::Portals,
            relations: RelationSnap::UNKNOWN,
        };
        bound(&mut observer, b"c", b"pc", commit);
        bound(&mut observer, b"s", b"ps", ONE);
        observer.admit_execute(Some(b"pc"), absent(), &pool);
        assert_eq!(statements(&pool), 1);
        observer.admit_execute(Some(b"ps"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);
        assert_eq!(observer.live(), (2, 0));
    }

    #[test]
    fn limits_are_128_statements_and_128_portals() {
        let mut observer = ClientObserver::new();
        for i in 0..SLOT_CAP {
            observer.stage_parse(format!("s{}", i).as_bytes(), ONE);
        }
        assert_eq!(observer.live().0, SLOT_CAP);
        assert!(!observer.quarantined);
        observer.stage_parse(b"one-too-many", ONE);
        assert!(observer.quarantined);
        assert_eq!(observer.live(), (0, 0));

        let mut observer = ClientObserver::new();
        for i in 0..SLOT_CAP {
            observer.stage_bind(format!("p{}", i).as_bytes(), b"");
        }
        assert_eq!(observer.live().1, SLOT_CAP);
        observer.stage_bind(b"one-too-many", b"");
        assert!(observer.quarantined);

        // Without a journal, a batch of any length fits.
        let mut observer = ClientObserver::new();
        for _ in 0..4 * SLOT_CAP {
            observer.stage_parse(b"s", ONE);
        }
        observer.send_batch();
        assert!(!observer.quarantined);
        assert_eq!(observer.live(), (1, 0));
    }

    #[test]
    fn replacing_names_at_full_capacity_does_not_overflow() {
        let mut observer = ClientObserver::new();
        for i in 0..SLOT_CAP {
            observer.stage_parse(format!("s{}", i).as_bytes(), ONE);
        }
        for i in 0..SLOT_CAP {
            observer.stage_bind(format!("p{}", i).as_bytes(), format!("s{}", i).as_bytes());
        }
        assert_eq!(observer.live(), (SLOT_CAP, SLOT_CAP));
        observer.send_batch();
        observer.server_replied(false, Some(b'T'));
        assert!(!observer.quarantined);
        assert_eq!(observer.portal_value(b"p7"), Some((ONE, false)));

        // Replace an existing statement and an existing portal: tables full,
        // no 129th entry, no quarantine.
        observer.stage_parse(b"s5", EMPTY);
        observer.stage_bind(b"p7", b"s5");
        assert!(!observer.quarantined);
        assert_eq!(observer.live(), (SLOT_CAP, SLOT_CAP));
        assert_eq!(observer.statement_meta(b"s5"), Some(EMPTY));
        assert_eq!(observer.portal_value(b"p7"), Some((EMPTY, false)));

        // A refusal takes nothing back: the replacements stand.
        observer.reject_batch();
        assert!(!observer.quarantined);
        assert_eq!(observer.live(), (SLOT_CAP, SLOT_CAP));
        assert_eq!(observer.statement_meta(b"s5"), Some(EMPTY));
        assert_eq!(observer.portal_value(b"p7"), Some((EMPTY, false)));

        // A genuinely new name still overflows.
        observer.stage_parse(b"one-too-many", ONE);
        assert!(observer.quarantined);
    }

    // A known portal's first Execute, an admitted lifecycle clear, then a local
    // refusal of the batch: nothing the clear removed comes back.
    #[test]
    fn lifecycle_clear_then_local_rejection_does_not_resurrect() {
        for both in [false, true] {
            let pool = pool();
            let mut observer = ClientObserver::new();
            let lifecycle = Snap::Known {
                count: 1,
                relations: RelationSnap::UNKNOWN,
                invalidate: if both {
                    Invalidate::Both
                } else {
                    Invalidate::Portals
                },
            };
            observer.stage_parse(b"A", ONE);
            observer.stage_parse(b"C", lifecycle);
            observer.stage_bind(b"P", b"A");
            observer.send_batch();
            observer.server_replied(false, Some(b'T'));
            assert_eq!(observer.portal_value(b"P"), Some((ONE, false)));

            // The new batch: P's first Execute, then the lifecycle statement.
            observer.admit_execute(Some(b"P"), absent(), &pool);
            observer.stage_bind(b"PC", b"C");
            observer.admit_execute(Some(b"PC"), absent(), &pool);
            assert_eq!(statements(&pool), 2);
            assert!(observer.portal_value(b"P").is_none());
            assert_eq!(observer.statement_meta(b"A").is_none(), both);

            observer.reject_batch();
            // Counts stay; nothing is resurrected.
            assert_eq!(statements(&pool), 2);
            assert!(observer.portal_value(b"P").is_none());
            assert!(observer.portal_value(b"PC").is_none());
            assert_eq!(observer.statement_meta(b"A").is_none(), both);
            assert_eq!(observer.statement_meta(b"C").is_none(), both);

            // The retry is a gap, not a second count.
            observer.admit_execute(Some(b"P"), absent(), &pool);
            assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);
            assert_eq!(statements(&pool), 2);

            if both {
                // Only a fresh Parse and Bind rebuild: a Bind alone is missing.
                observer.stage_bind(b"P", b"A");
                observer.admit_execute(Some(b"P"), absent(), &pool);
                assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 2);
                assert_eq!(statements(&pool), 2);
                observer.stage_parse(b"A", ONE);
                observer.stage_bind(b"P", b"A");
            } else {
                // The statements stayed: a fresh Bind is enough.
                observer.stage_bind(b"P", b"A");
            }
            observer.admit_execute(Some(b"P"), absent(), &pool);
            assert_eq!(statements(&pool), 3);
        }
    }

    #[test]
    fn names_of_128_bytes_are_kept_and_129_overflow() {
        let long = [b'n'; NAME_CAP];
        let mut observer = ClientObserver::new();
        observer.stage_parse(&long, ONE);
        assert!(!observer.quarantined);
        assert!(observer.statement_meta(&long).is_some());
        observer.stage_parse(&[b'n'; NAME_CAP + 1], ONE);
        assert!(observer.quarantined);

        // Closing a name too long to be kept is nothing.
        let mut observer = ClientObserver::new();
        observer.stage_close(&[b'n'; NAME_CAP + 1], true);
        assert!(!observer.quarantined);

        // Executing one is an ordinary missing portal.
        let pool = pool();
        observer.admit_execute(None, absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::UnknownPortal), 1);
    }

    #[test]
    fn overflow_after_a_counted_execute_keeps_the_count_and_recovers_forward() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        bound(&mut observer, b"s", b"p", ONE);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        observer.send_batch();
        for i in 0..=SLOT_CAP {
            observer.stage_parse(format!("x{}", i).as_bytes(), ONE);
        }
        assert!(observer.quarantined);
        assert_eq!(statements(&pool), 1);

        // Nothing is kept while quarantined; the sent batch's answer, then
        // the end of the batch being staged, let it recover.
        observer.reject_batch();
        assert!(observer.quarantined);
        observer.server_replied(false, Some(b'I'));
        assert!(!observer.quarantined);

        bound(&mut observer, b"s", b"p", ONE);
        observer.send_batch();
        observer.server_replied(false, Some(b'T'));
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(statements(&pool), 2);
    }

    #[test]
    fn disabled_observers_have_no_storage() {
        let client: Option<Box<ClientObserver>> = None;
        assert_eq!(std::mem::size_of_val(&client), std::mem::size_of::<usize>());
        assert!(!SqlObserver::new(false, &[], None, &id("db")).enabled());
    }

    #[test]
    fn actual_layout_sizes_are_within_the_budget() {
        let state = std::mem::size_of::<Option<StatementEntry>>() * SLOT_CAP
            + std::mem::size_of::<Option<PortalEntry>>() * SLOT_CAP
            + std::mem::size_of::<ClientObserver>();
        assert!(state <= 16_512, "{}", state);
        let observer = ClientObserver::new();
        assert_eq!(observer.statements.len(), SLOT_CAP);
        assert_eq!(observer.portals.len(), SLOT_CAP);
    }

    const EMPTY: Snap = Snap::Known {
        count: 0,
        invalidate: Invalidate::Nothing,
        relations: RelationSnap::UNKNOWN,
    };

    // Trace 1: Flush seals a batch with a counted Execute; the next batch
    // replaces the portal, executes it, and is refused locally (or the backend
    // errors first).
    #[test]
    fn refused_and_failed_batches_stand() {
        for error_before_rejection in [false, true] {
            let pool = pool();
            let mut observer = ClientObserver::new();
            // Known statement A, from a batch already answered.
            observer.stage_parse(b"A", ONE);
            observer.send_batch();
            observer.server_replied(false, Some(b'T'));

            // Bind P to A and the first Execute, sealed by the Flush.
            observer.stage_bind(b"P", b"A");
            observer.admit_execute(Some(b"P"), absent(), &pool);
            observer.send_batch();
            assert_eq!(statements(&pool), 1);

            // New staging: Parse B, replacement Bind P to B, Execute.
            observer.stage_parse(b"B", ONE);
            observer.stage_bind(b"P", b"B");
            observer.admit_execute(Some(b"P"), absent(), &pool);
            assert_eq!(statements(&pool), 2);
            assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));

            if error_before_rejection {
                // A backend error forgets nothing.
                observer.server_replied(true, None);
                assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));
            }

            observer.reject_batch();
            // Nothing is taken back, and the admitted counts stay.
            assert_eq!(statements(&pool), 2, "both admitted counts stay");
            assert!(observer.statement_meta(b"B").is_some());
            assert!(observer.statement_meta(b"A").is_some());
            assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));
            observer.server_replied(true, None);
            assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));

            // Retrying the executed generation counts nothing.
            observer.admit_execute(Some(b"P"), absent(), &pool);
            assert_eq!(statements(&pool), 2);
            observer.server_replied(false, Some(b'T'));
            assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));
        }
    }

    // A portal generation bound in an accepted batch stays counted when a later
    // batch that executed it is refused.
    #[test]
    fn refused_batch_keeps_a_surviving_portal_generation_executed() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        observer.stage_parse(b"A", ONE);
        observer.stage_bind(b"P", b"A");
        observer.accept_local_batch();

        observer.admit_execute(Some(b"P"), absent(), &pool);
        observer.stage_parse(b"B", ONE);
        observer.reject_batch();
        assert_eq!(statements(&pool), 1);
        assert_eq!(observer.portal_value(b"P"), Some((ONE, true)));

        // Retrying the Execute of the same generation counts nothing.
        observer.admit_execute(Some(b"P"), absent(), &pool);
        assert_eq!(statements(&pool), 1);

        // A fresh Bind is a new generation and counts again.
        observer.stage_bind(b"P", b"A");
        observer.admit_execute(Some(b"P"), absent(), &pool);
        assert_eq!(statements(&pool), 2);
    }

    // Trace 2: a Query while Parse and Bind are staged and unsent.
    #[test]
    fn trace_query_while_parse_and_bind_are_pending() {
        for idle in [false, true] {
            let pool = pool();
            let mut observer = ClientObserver::new();
            // Committed S=old and P=old.
            observer.stage_parse(b"S", ONE);
            observer.stage_bind(b"P", b"S");
            observer.send_batch();
            observer.server_replied(false, Some(b'T'));
            assert_eq!(observer.portal_value(b"P"), Some((ONE, false)));

            // Pending, unsent: S=new and a replacement P.
            observer.stage_parse(b"S", EMPTY);
            observer.stage_bind(b"P", b"S");
            assert_eq!(observer.portal_value(b"P"), Some((EMPTY, false)));

            pool.attempts(1, Caller::anonymous(IdentityState::Absent));
            observer.admit_query(QueryAdmission::Parsed(Invalidate::Nothing));
            observer.send_query();
            assert_eq!(statements(&pool), 1, "only the Query counts");

            // Its error: the pending Parse and Bind stand.
            observer.server_replied(true, None);
            assert_eq!(observer.statement_meta(b"S"), Some(EMPTY));
            assert_eq!(observer.portal_value(b"P"), Some((EMPTY, false)));

            observer.server_replied(false, Some(if idle { b'I' } else { b'T' }));
            // A refusal takes nothing back.
            observer.reject_batch();
            assert_eq!(observer.statement_meta(b"S"), Some(EMPTY));
            if idle {
                // Idle dropped the portals.
                assert!(observer.portal_value(b"P").is_none());
            } else {
                assert_eq!(observer.portal_value(b"P"), Some((EMPTY, false)));
            }
        }
    }

    #[test]
    fn idle_resets_the_reason_even_while_quarantine_stays_open() {
        let pool = pool();
        let mut observer = ClientObserver::new();
        observer.stage_parse(b"A", ONE);
        observer.send_batch();
        for i in 0..=SLOT_CAP {
            observer.stage_parse(format!("x{}", i).as_bytes(), ONE);
        }
        assert!(observer.quarantined);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);

        // The sent batch is answered idle; the batch being staged still keeps
        // the quarantine open.
        observer.server_replied(false, Some(b'I'));
        assert!(observer.quarantined);
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(gap_total(&pool, Gap::UnknownPortal), 1);
        assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);
    }

    #[test]
    fn a_refused_close_stands() {
        let mut observer = ClientObserver::new();
        bound(&mut observer, b"s", b"p", ONE);
        observer.accept_local_batch();
        observer.stage_close(b"s", false);
        observer.stage_close(b"p", true);
        assert_eq!(observer.live(), (0, 0));
        observer.reject_batch();
        assert_eq!(observer.live(), (0, 0));
    }

    #[test]
    fn name_keys_keep_the_unnamed_statement_and_portal_apart() {
        assert!(NameKey::new(b"") == Some(NameKey::UNNAMED));
        assert!(NameKey::new(b"s") == NameKey::new(b"s"));
        assert!(NameKey::new(b"s") != NameKey::new(b"p"));
        assert!(NameKey::new(b"\0") != Some(NameKey::UNNAMED));

        let pool = pool();
        let mut observer = ClientObserver::new();
        bound(&mut observer, b"", b"", ONE);
        bound(&mut observer, b"s", b"p", ONE);
        assert!(observer.statement_meta(b"").is_some());
        assert!(observer.portal_value(b"").is_some());
        observer.admit_execute(Some(b""), absent(), &pool);
        assert_eq!(statements(&pool), 1);
        observer.accept_local_batch();
        // A simple Query drops the unnamed pair only.
        observer.admit_query(QueryAdmission::Parsed(Invalidate::Nothing));
        assert!(observer.statement_meta(b"").is_none());
        assert!(observer.portal_value(b"").is_none());
        assert!(observer.statement_meta(b"s").is_some());
        assert!(observer.portal_value(b"p").is_some());
    }

    #[test]
    fn query_admission_clears_unnamed_and_what_the_statement_ends() {
        let mut observer = ClientObserver::new();
        bound(&mut observer, b"", b"", ONE);
        bound(&mut observer, b"s", b"p", ONE);
        observer.accept_local_batch();
        observer.admit_query(QueryAdmission::Parsed(Invalidate::Nothing));
        assert_eq!(observer.live(), (1, 1));
        observer.admit_query(QueryAdmission::Parsed(Invalidate::Portals));
        assert_eq!(observer.live(), (1, 0));
        observer.admit_query(QueryAdmission::Unparsed);
        assert_eq!(observer.live(), (0, 0));
    }

    #[test]
    fn name_extraction_reads_the_frames_without_copying() {
        let bind = b"B\0\0\0\x10pp\0ss\0\0\0\0\0\0\0";
        assert_eq!(bind_names(bind), Some((&b"pp"[..], &b"ss"[..])));
        assert_eq!(close_target(b"C\0\0\0\x08Px\0"), Some((true, &b"x"[..])));
        assert_eq!(parse_name(b"P\0\0\0\x08n\0"), Some(&b"n"[..]));
        assert_eq!(execute_portal(b"E\0\0\0\x09\0\0\0\0\0"), Some(&b""[..]));
        assert_eq!(close_target(b"C\0\0\0\x08Zx\0"), None);
    }
    use super::*;

    fn id(db: &str) -> PoolIdentifier {
        PoolIdentifier::new(db, "app")
    }

    #[test]
    fn disabled_counts_nothing_and_says_so() {
        let observer = SqlObserver::default();
        observer.attempts(3, Caller::anonymous(IdentityState::Present));
        observer.gap(Gap::UnparsedQuery, absent());

        let lines = render(&[(&id("db"), &observer)]);

        assert!(lines.contains(&format!("{}{{pool=\"db\",user=\"app\"}} 0", OBSERVING)));
        assert!(!lines.iter().any(|line| line.starts_with(ATTEMPTS)));
        assert!(!lines.iter().any(|line| line.starts_with(GAPS)));
        assert_eq!(
            observer.counters.identities[OTHER_CLIENT][IdentityState::Present.column()]
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            observer.counters.gaps[OTHER_CLIENT][0].load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn counts_statements_by_identity_and_gaps_by_reason() {
        let observer = SqlObserver::new(true, &[], None, &id("db"));
        observer.attempts(3, Caller::anonymous(IdentityState::Present));
        observer.attempts(2, Caller::anonymous(IdentityState::Absent));
        observer.attempts(1, Caller::anonymous(IdentityState::Absent));
        observer.gap(Gap::UnknownPortal, absent());
        observer.gap(Gap::RefusedUnparsed, absent());
        observer.gap(Gap::RefusedUnparsed, absent());

        let lines = render(&[(&id("db"), &observer)]);
        let labels = "pool=\"db\",user=\"app\"";

        for expected in [
            format!("{}{{{}}} 1", OBSERVING, labels),
            format!(
                "{}{{{},client=\"other\",identity=\"present\"}} 3",
                ATTEMPTS, labels
            ),
            format!(
                "{}{{{},client=\"other\",identity=\"absent\"}} 3",
                ATTEMPTS, labels
            ),
            format!(
                "{}{{{},client=\"other\",identity=\"expired\"}} 0",
                ATTEMPTS, labels
            ),
            format!(
                "{}{{{},client=\"other\",identity=\"refused\"}} 0",
                ATTEMPTS, labels
            ),
            format!(
                "{}{{{},client=\"other\",reason=\"unknown_portal\"}} 1",
                GAPS, labels
            ),
            format!(
                "{}{{{},client=\"other\",reason=\"unparsed_query\"}} 0",
                GAPS, labels
            ),
            format!(
                "{}{{{},client=\"other\",reason=\"refused_unparsed\"}} 2",
                GAPS, labels
            ),
        ] {
            assert!(
                lines.contains(&expected),
                "{} not in {:#?}",
                expected,
                lines
            );
        }
    }

    #[test]
    fn counts_by_the_configured_clients() {
        let names =
            |names: &[&str]| -> Vec<String> { names.iter().map(|n| n.to_string()).collect() };
        let caller = |application_name| Caller {
            identity: IdentityState::Absent,
            application_name,
            address: None,
        };
        let labels = "pool=\"db\",user=\"app\"";
        let attempts = |client: &str, count: u64| {
            format!(
                "{}{{{},client=\"{}\",identity=\"absent\"}} {}",
                ATTEMPTS, labels, client, count
            )
        };

        // Unconfigured, every name is other.
        let unconfigured = pool();
        unconfigured.attempts(3, caller("app-a"));
        let lines = render(&[(&id("db"), &unconfigured)]);
        assert!(lines.contains(&attempts("other", 3)), "{:#?}", lines);

        let observer = SqlObserver::new(true, &names(&["app-a", "app-b"]), None, &id("db"));
        observer.attempts(2, caller("app-a"));
        observer.attempts(1, caller("app-a-canary"));
        observer.attempts(1, caller("App-A"));
        observer.gap(Gap::UnknownPortal, caller("app-b"));
        // Reloaded without app-b: app-a keeps its column, app-b its series
        // (an old client still counts to it), and app-c takes a new column.
        let reloaded = SqlObserver::new(
            true,
            &names(&["app-c", "app-a"]),
            Some(&observer),
            &id("db"),
        );
        reloaded.attempts(1, caller("app-a"));
        reloaded.attempts(4, caller("app-c"));
        reloaded.attempts(1, caller("app-b"));
        observer.attempts(1, caller("app-b"));

        let lines = render(&[(&id("db"), &reloaded)]);
        for expected in [
            attempts("app-a", 3),
            attempts("other", 3),
            attempts("app-b", 1),
            attempts("app-c", 4),
            format!(
                "{}{{{},client=\"app-b\",reason=\"unknown_portal\"}} 1",
                GAPS, labels
            ),
        ] {
            assert!(
                lines.contains(&expected),
                "{} not in {:#?}",
                expected,
                lines
            );
        }

        assert_eq!(invalid_clients(&names(&["app-a", "app-b"])), None);
        for invalid in [
            names(&["app-a", "app-a"]),
            names(&["other"]),
            names(&["app-a,app-b"]),
            names(&[""]),
            names(&[&"a".repeat(MAX_CLIENT_NAME_BYTES + 1)]),
            (0..=MAX_CLIENTS).map(|n| format!("app-{}", n)).collect(),
        ] {
            assert!(invalid_clients(&invalid).is_some(), "{:?}", invalid);
        }
    }

    #[test]
    fn client_lineage_is_capped_and_reused() {
        let caller = |application_name| Caller {
            identity: IdentityState::Absent,
            application_name,
            address: None,
        };
        let names = |r: std::ops::Range<usize>| -> Vec<String> {
            r.map(|n| format!("app-{}", n)).collect()
        };
        assert_eq!(invalid_clients(&names(0..MAX_CLIENTS)), None);
        let all = names(0..MAX_CLIENTS);
        let a = SqlObserver::new(true, &all, None, &id("db"));
        for n in &all {
            a.attempts(1, caller(n));
        }
        // Drop app-0, add a 33rd lifetime name: it counts as other.
        let b = SqlObserver::new(true, &names(1..MAX_CLIENTS + 1), Some(&a), &id("db"));
        b.attempts(5, caller("app-32"));
        // Re-add app-0: it keeps its column and counts.
        let c = SqlObserver::new(true, &all, Some(&b), &id("db"));
        c.attempts(2, caller("app-0"));
        let lines = render(&[(&id("db"), &c)]);
        let absent: Vec<_> = lines
            .iter()
            .filter(|l| l.starts_with(ATTEMPTS) && l.contains("identity=\"absent\""))
            .collect();
        assert_eq!(absent.len(), MAX_CLIENTS + 1, "{:#?}", absent);
        assert!(lines
            .iter()
            .any(|l| l.contains("client=\"app-0\",identity=\"absent\"} 3")));
        assert!(lines
            .iter()
            .any(|l| l.contains("client=\"other\",identity=\"absent\"} 5")));
        assert!(!lines.iter().any(|l| l.contains("app-32")));
        assert_eq!(c.counters.clients.lock().len(), MAX_CLIENTS);
    }

    #[test]
    fn defaults_share_one_placeholder() {
        let (a, b) = (SqlObserver::default(), SqlObserver::default());
        assert!(Arc::ptr_eq(&a.counters, &b.counters));
        assert!(!Arc::ptr_eq(&a.counters, &pool().counters));
    }

    #[test]
    fn reload_keeps_the_counts() {
        let before = SqlObserver::new(true, &[], None, &id("db"));
        before.attempts(2, Caller::anonymous(IdentityState::Absent));

        let disabled = SqlObserver::new(false, &[], Some(&before), &id("db"));
        disabled.attempts(5, Caller::anonymous(IdentityState::Absent));
        let enabled = SqlObserver::new(true, &[], Some(&disabled), &id("db"));
        enabled.attempts(1, Caller::anonymous(IdentityState::Absent));
        // A client still holding the settings it started with.
        before.attempts(1, Caller::anonymous(IdentityState::Absent));

        assert_eq!(
            enabled.counters.identities[OTHER_CLIENT][IdentityState::Absent.column()]
                .load(Ordering::Relaxed),
            4
        );
    }

    #[test]
    fn pool_names_are_escaped() {
        let observer = SqlObserver::default();
        let lines = render(&[(&PoolIdentifier::new("a\"b\\c", "u\nv"), &observer)]);

        assert!(lines.contains(&format!(
            "{}{{pool=\"a\\\"b\\\\c\",user=\"u\\nv\"}} 0",
            OBSERVING
        )));
    }

    // --- Relations (Stage C) ---

    use crate::relations::{Gate, FLAG_OTHER};

    fn present() -> Caller<'static> {
        Caller::anonymous(IdentityState::Present)
    }

    fn absent() -> Caller<'static> {
        Caller::anonymous(IdentityState::Absent)
    }

    /// An enabled pool whose hub is published, and the gate that published it.
    fn published() -> (SqlObserver, Gate) {
        let pool = pool();
        let mut gate = Gate::new();
        gate.install(pool.hub());
        (pool, gate)
    }

    fn relation_snap(pool: &SqlObserver, sql: &str) -> RelationSnap {
        let ast =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)
                .unwrap();
        ClientObserver::new().classify_parse(pool, &ast)
    }

    fn known(relations: RelationSnap) -> Snap {
        Snap::Known {
            count: 1,
            invalidate: Invalidate::Nothing,
            relations,
        }
    }

    fn rel(pool: &SqlObserver, label: &str) -> [u64; IdentityState::COUNT] {
        pool.hub()
            .samples()
            .into_iter()
            .find(|(name, _)| name == label)
            .map(|(_, v)| v)
            .unwrap_or_default()
    }

    #[test]
    fn layouts_hold_the_budget() {
        assert!(std::mem::size_of::<Snap>() <= 48);
        assert!(std::mem::size_of::<RelationSnap>() <= 40);
        assert!(std::mem::size_of::<Option<StatementEntry>>() <= 64);
        assert!(std::mem::size_of::<Option<PortalEntry>>() <= 64);
        assert!(std::mem::size_of::<ClientObserver>() <= 128);
        let total = SLOT_CAP * std::mem::size_of::<Option<StatementEntry>>()
            + SLOT_CAP * std::mem::size_of::<Option<PortalEntry>>()
            + std::mem::size_of::<ClientObserver>();
        assert!(total <= 16_512, "{total}");
    }

    #[test]
    fn first_execute_counts_relations_once_under_its_identity() {
        let (pool, _gate) = published();
        let mut observer = ClientObserver::new();
        let snap = relation_snap(&pool, "SELECT * FROM x.a JOIN x.b USING (id)");
        bound(&mut observer, b"s", b"p", known(snap));
        assert_eq!(rel(&pool, "a"), [0; 4]);
        observer.admit_execute(Some(b"p"), present(), &pool);
        assert_eq!(
            (rel(&pool, "a"), rel(&pool, "b")),
            ([1, 0, 0, 0], [1, 0, 0, 0])
        );
        // Continuation: nothing, even under another identity.
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!((rel(&pool, "a"), statements(&pool)), ([1, 0, 0, 0], 1));
        // A fresh Bind counts again, under the identity of its first Execute.
        observer.stage_bind(b"p2", b"s");
        observer.admit_execute(
            Some(b"p2"),
            Caller::anonymous(IdentityState::Expired),
            &pool,
        );
        assert_eq!(
            (rel(&pool, "a"), rel(&pool, "b")),
            ([1, 0, 1, 0], [1, 0, 1, 0])
        );
        assert_eq!(
            pool.counters.identities[OTHER_CLIENT][IdentityState::Expired.column()]
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn relation_overflow_and_another_lineage_are_unknown_without_gaps() {
        let (pool, mut gate) = published();
        let mut observer = ClientObserver::new();
        let many: Vec<String> = (0..17).map(|i| format!("y.t{i}")).collect();
        let overflow = relation_snap(&pool, &format!("SELECT * FROM {}", many.join(", ")));
        bound(&mut observer, b"s", b"p", known(overflow));
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!(statements(&pool), 1);
        assert_eq!(rel(&pool, "unknown"), [0, 1, 0, 0]);
        assert!(Gap::ALL.iter().all(|gap| gap_total(&pool, *gap) == 0));
        // Classified before the pool was removed, admitted after it came back.
        let old = relation_snap(&pool, "SELECT * FROM x.a");
        gate.publish(&[pool.hub()], &[]);
        gate.install(pool.hub());
        bound(&mut observer, b"s2", b"p2", known(old));
        observer.admit_execute(Some(b"p2"), present(), &pool);
        assert_eq!(statements(&pool), 2);
        assert_eq!(rel(&pool, "unknown"), [1, 0, 0, 0]);
        assert_eq!(rel(&pool, "a"), [0; 4]);
        assert!(Gap::ALL.iter().all(|gap| gap_total(&pool, *gap) == 0));
    }

    #[test]
    fn prepared_metadata_keeps_its_slots_across_republication() {
        let (pool, mut gate) = published();
        let mut observer = ClientObserver::new();
        let old = relation_snap(&pool, "SELECT * FROM x.a");
        bound(&mut observer, b"s", b"p", known(old));
        observer.admit_execute(Some(b"p"), present(), &pool);
        assert_eq!(rel(&pool, "a"), [1, 0, 0, 0]);
        // A reload that keeps the pool keeps its epoch and slots.
        gate.publish(&[pool.hub()], &[pool.hub()]);
        observer.admit_execute(Some(b"p"), present(), &pool);
        assert_eq!(statements(&pool), 1);
        observer.stage_bind(b"p2", b"s");
        observer.admit_execute(Some(b"p2"), present(), &pool);
        assert_eq!((statements(&pool), rel(&pool, "a")), (2, [2, 0, 0, 0]));
        assert_eq!(rel(&pool, "unknown"), [0; 4]);
    }

    #[test]
    fn empty_and_disabled_admissions_add_no_relations() {
        let (pool, _gate) = published();
        let mut observer = ClientObserver::new();
        bound(
            &mut observer,
            b"s",
            b"p",
            Snap::Known {
                count: 0,
                invalidate: Invalidate::Nothing,
                relations: RelationSnap::UNKNOWN,
            },
        );
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert!(pool.hub().samples().iter().all(|(_, v)| *v == [0; 4]));
        let off = SqlObserver::new(false, &[], None, &id("db"));
        off.relation_attempt(RelationSnap::UNKNOWN, present());
        assert!(off.hub().samples().iter().all(|(_, v)| *v == [0; 4]));
    }

    #[test]
    fn relation_payload_after_refusal_replacement_and_lifecycle() {
        let (pool, _gate) = published();
        let a = relation_snap(&pool, "SELECT * FROM x.a");
        let b = relation_snap(&pool, "SELECT * FROM x.b");
        assert_ne!(a, b);
        let mut observer = ClientObserver::new();
        // The first Parse is sent; a refused batch replaces its name, and the
        // refused definition stands: the name is attributed to it.
        observer.stage_parse(b"s", known(a));
        observer.send_batch();
        observer.stage_parse(b"s", known(b));
        observer.reject_batch();
        observer.stage_bind(b"p", b"s");
        observer.admit_execute(Some(b"p"), absent(), &pool);
        assert_eq!((rel(&pool, "a"), rel(&pool, "b")), ([0; 4], [0, 1, 0, 0]));
        // Redefined, it is bound with the new payload.
        observer.stage_parse(b"s", known(a));
        observer.stage_bind(b"p2", b"s");
        observer.admit_execute(Some(b"p2"), absent(), &pool);
        assert_eq!(
            (rel(&pool, "a"), rel(&pool, "b")),
            ([0, 1, 0, 0], [0, 1, 0, 0])
        );
        // A lifecycle clear stands through a rejection: a gap, no relation.
        observer.stage_parse(
            b"d",
            Snap::Known {
                count: 1,
                invalidate: Invalidate::Both,
                relations: RelationSnap::UNKNOWN,
            },
        );
        observer.stage_bind(b"pd", b"d");
        observer.admit_execute(Some(b"pd"), absent(), &pool);
        let unknown = rel(&pool, "unknown");
        observer.reject_batch();
        observer.stage_bind(b"p3", b"s");
        observer.admit_execute(Some(b"p3"), absent(), &pool);
        assert_eq!(rel(&pool, "unknown"), unknown);
        assert_eq!(
            (rel(&pool, "a"), rel(&pool, "b")),
            ([0, 1, 0, 0], [0, 1, 0, 0])
        );
        assert_eq!(gap_total(&pool, Gap::MetadataInvalidated), 1);
    }

    #[test]
    fn quote_heavy_names_cut_to_one_label_share_one_slot_and_series() {
        let (pool, _gate) = published();
        // 63 bytes each, distinct to PostgreSQL, but their quotes double past
        // the 64-character cap before they differ.
        let a = format!("{}{}", "\"".repeat(40), "a".repeat(23));
        let b = format!("{}{}", "\"".repeat(40), "b".repeat(23));
        assert_eq!((a.len(), b.len()), (63, 63));
        let sql = |name: &str| format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""));
        let first = relation_snap(&pool, &sql(&a));
        let second = relation_snap(&pool, &sql(&b));
        assert_eq!((first.slots(), second.slots()), (&[0u16][..], &[0u16][..]));
        let label = "\"".repeat(64);
        assert_eq!(pool.hub().current().2, [label.clone()]);
        pool.relation_attempt(first, absent());
        pool.relation_attempt(second, absent());
        let id = PoolIdentifier::new("db", "u");
        let mut lines = Vec::new();
        render_relations(&mut lines, &[(&id, pool.hub().samples())]);
        let series: Vec<&str> = lines[2..]
            .iter()
            .map(|line| line.rsplit_once(' ').unwrap().0)
            .collect();
        let distinct: std::collections::HashSet<&str> = series.iter().copied().collect();
        assert_eq!(distinct.len(), series.len(), "{lines:#?}");
        let escaped = format!("relation=\"{}\"", "\\\"".repeat(64));
        let ours: Vec<&String> = lines
            .iter()
            .filter(|line| line.contains(&escaped))
            .collect();
        assert_eq!(ours.len(), IdentityState::COUNT, "{ours:#?}");
        for (identity, value) in [
            ("present", 0),
            ("absent", 2),
            ("expired", 0),
            ("refused", 0),
        ] {
            let line = format!("{escaped},identity=\"{identity}\"}} {value}");
            assert!(
                ours.iter().any(|l| l.ends_with(&line)),
                "{line} not in {ours:#?}"
            );
        }
    }

    #[test]
    fn relation_metric_renders_bare_and_escaped_labels_for_every_identity() {
        let (pool, _gate) = published();
        let snap = relation_snap(&pool, r#"SELECT * FROM "a\b", "C""d,e", Public.Orders"#);
        pool.relation_attempt(snap, present());
        let mut other = RelationSnap::UNKNOWN;
        other.epoch = pool.hub().current().0;
        other.flags = FLAG_OTHER;
        pool.relation_attempt(other, Caller::anonymous(IdentityState::Refused));
        let id = PoolIdentifier::new("db", "u");
        let mut lines = Vec::new();
        render_relations(&mut lines, &[(&id, pool.hub().samples())]);
        assert!(lines[1].ends_with("counter"));
        let body = lines[2..].join("\n");
        for expected in [
            r#"relation="\"a\\b\"",identity="present"} 1"#,
            r#"relation="\"C\"\"d,e\"",identity="present"} 1"#,
            r#"relation="orders",identity="present"} 1"#,
            r#"relation="orders",identity="expired"} 0"#,
            r#"relation="other",identity="refused"} 1"#,
        ] {
            assert!(body.contains(expected), "{expected} not in {body}");
        }
        // Seen names, then the three fixed labels, each for every identity.
        assert_eq!(lines.len() - 2, (3 + 3) * IdentityState::COUNT);
    }
}
