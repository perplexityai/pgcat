//! Which relations (tables) the SQL statements a pool admits name, for the
//! SQL observer's `relation_attempts` metric.
//!
//! Every relation a statement names is labeled by its bare table name (the
//! schema is dropped): `orders`, `public.orders` and `archive.orders` are all
//! `orders`. The search path is never consulted, so false positives (a temp
//! table, the same name in another schema) are intended and misses are not;
//! the database is the pool's own label.
//!
//! Each pool lineage (one `(pool, user)` across reloads) has a `RelationHub`
//! that assigns table names to counter slots first-seen, up to
//! `MAX_REGISTRY`. Slots are append-only for as long as the hub stays
//! published: no reload moves or evicts one, so a snapshot's slot ids stay
//! valid for the hub's epoch. Names past the cap count as `other`. Slot
//! storage is allocated `CHUNK_SLOTS` names at a time as names arrive, and
//! never moves.
//!
//! A statement's relations are classified from its original AST into an
//! immutable `RelationSnap` (slot ids plus `other` / `unknown` /
//! `no_explicit_relation` flags) stamped with the hub's epoch. Admission
//! counts a snapshot only while the hub still has that epoch: a snapshot from
//! another lineage (a removed pool's hub, or one republished after removal)
//! counts as `unknown`. Nothing here changes what pgcat sends, parses, routes
//! or refuses.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use log::{info, warn};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use sqlparser::ast::{
    CopySource, Delete, Expr, FromTable, Ident, ObjectName, Query, Set, SetExpr, Statement, Table,
    TableFactor, TableObject, Update, Visit, Visitor,
};

use crate::identity_label::IdentityState;

/// Distinct table names one hub labels; later names count as `other`.
pub const MAX_REGISTRY: usize = 1000;
/// A hub warns once when it has this many names (80% of the cap).
pub const WARN_REGISTRY: usize = MAX_REGISTRY * 4 / 5;
/// Longest identifier, in bytes: PostgreSQL's (`NAMEDATALEN - 1`). A longer
/// name is cut to it, on a character boundary, as PostgreSQL cuts it.
pub const MAX_IDENT_BYTES: usize = 63;
/// Longest label, in characters, quotes included.
pub const MAX_LABEL_CHARS: usize = 64;
/// Longest label, in bytes: a cut identifier quoted with every character a
/// doubled quote.
pub const MAX_NAME_BYTES: usize = 2 * MAX_IDENT_BYTES + 2;
/// Distinct relation keys one statement may name before it is classified unknown.
pub const MAX_KEYS: usize = 16;
/// Visible and pending CTE aliases while one statement is classified.
pub const MAX_ALIASES: usize = 64;
/// Query scopes, and the depth of the explicit set-operation scan.
pub const MAX_SCOPES: usize = 32;
/// Expression nesting the classifier follows.
pub const MAX_EXPR_DEPTH: usize = 64;
/// Classifier scratch budget, in bytes.
pub const SCRATCH_BUDGET: usize = 16_384;
/// At most one overflow line per hub this often...
pub const OVERFLOW_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// ...naming at most this many of the names that overflowed since the last one.
pub const OVERFLOW_LOG_NAMES: usize = 16;
/// The log target of the first sighting of each relation by a client without
/// an identity, for filtering.
pub const CALLERS_LOG_TARGET: &str = "pgcat::sql_observer::callers";

/// Slot counters are allocated this many slots at a time, as names arrive.
pub const CHUNK_SLOTS: usize = 64;
/// Chunks enough for `MAX_REGISTRY` slots.
pub const CHUNKS: usize = MAX_REGISTRY.div_ceil(CHUNK_SLOTS);

/// The fixed rows: `other`, `unknown`, `no_explicit_relation`.
const OTHER: usize = 0;
const UNKNOWN: usize = 1;
const NO_EXPLICIT: usize = 2;

/// Labels a table name can't take: one with such a name is quoted.
const FIXED_LABELS: [&str; 3] = ["other", "unknown", "no_explicit_relation"];

/// Snapshot flags. `FLAG_NO_EXPLICIT` is exclusive of the other two.
pub const FLAG_OTHER: u8 = 1;
pub const FLAG_UNKNOWN: u8 = 2;
pub const FLAG_NO_EXPLICIT: u8 = 4;

/// The relations of one statement: `slots[..len]` of the hub with `epoch`.
/// Epoch 0 is never a live hub: such a snapshot always counts as unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelationSnap {
    pub epoch: u32,
    pub slots: [u16; MAX_KEYS],
    pub len: u8,
    pub flags: u8,
}

impl RelationSnap {
    /// No usable classification.
    pub const UNKNOWN: RelationSnap = RelationSnap {
        epoch: 0,
        slots: [0; MAX_KEYS],
        len: 0,
        flags: FLAG_UNKNOWN,
    };

    pub fn slots(&self) -> &[u16] {
        &self.slots[..self.len as usize]
    }
}

const _: () = assert!(std::mem::size_of::<RelationSnap>() <= 40);
const _: () = assert!(MAX_REGISTRY <= u16::MAX as usize);

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

/// A table name's label, built in place without allocating.
#[derive(Clone, Copy)]
pub struct KeyBuf {
    len: u8,
    bytes: [u8; MAX_NAME_BYTES],
}

impl PartialEq for KeyBuf {
    fn eq(&self, other: &KeyBuf) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

/// `value`'s first `max` bytes at most, cut on a character boundary.
fn clip(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

/// Whether PostgreSQL reads `content` unquoted as itself.
fn bare(content: &str) -> bool {
    let bytes = content.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'$')
}

impl KeyBuf {
    pub const EMPTY: KeyBuf = KeyBuf {
        len: 0,
        bytes: [0; MAX_NAME_BYTES],
    };

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    fn push_byte(&mut self, byte: u8) -> bool {
        if self.len as usize == MAX_NAME_BYTES {
            return false;
        }
        self.bytes[self.len as usize] = byte;
        self.len += 1;
        true
    }

    /// The label of a name whose value (as the server reads it) is `value`:
    /// itself when it needs no quotes and isn't a fixed label, else
    /// `"value"` with embedded quotes doubled.
    ///
    /// The value is first cut to `MAX_IDENT_BYTES`, so names PostgreSQL
    /// reads as one identifier (sharing their first 63 bytes) get one label.
    /// The label is then cut to `MAX_LABEL_CHARS`, which can merge names
    /// PostgreSQL keeps apart, deliberately (a false positive):
    /// - A quoted name of 63 one-byte characters, and no embedded quotes,
    ///   loses only its closing quote. That merges nothing.
    /// - Each embedded quote is doubled, so a name loses real content once
    ///   its characters plus its embedded quotes exceed 63: one embedded
    ///   quote in a 63-character name is enough. Two names that differ only
    ///   past the cut get one label.
    ///
    /// The label is the registry key, so names sharing a label share one slot
    /// and one series: no two slots ever render the same label.
    fn label(value: &str) -> Option<KeyBuf> {
        let value = clip(value, MAX_IDENT_BYTES);
        if value.is_empty() || value.as_bytes().contains(&0) {
            return None;
        }
        let mut key = KeyBuf::EMPTY;
        if bare(value) && !FIXED_LABELS.contains(&value) {
            // At most 63 ASCII bytes: within both caps.
            for &byte in value.as_bytes() {
                if !key.push_byte(byte) {
                    return None;
                }
            }
            return Some(key);
        }
        let mut chars = 0;
        let mut push = |key: &mut KeyBuf, c: char| -> bool {
            if chars == MAX_LABEL_CHARS || key.len as usize + c.len_utf8() > MAX_NAME_BYTES {
                return false;
            }
            let mut buf = [0u8; 4];
            for &byte in c.encode_utf8(&mut buf).as_bytes() {
                key.push_byte(byte);
            }
            chars += 1;
            true
        };
        push(&mut key, '"');
        for c in value.chars() {
            if (c == '"' && !push(&mut key, '"')) || !push(&mut key, c) {
                return Some(key);
            }
        }
        push(&mut key, '"');
        Some(key)
    }

    /// An identifier's label: unquoted ASCII folds to lower case (unquoted
    /// non-ASCII is refused), `"` quoting is kept as written, other quote
    /// styles are refused.
    fn from_ident(ident: &Ident) -> Option<KeyBuf> {
        match ident.quote_style {
            None if ident.value.is_ascii() => {
                let mut folded = [0u8; MAX_IDENT_BYTES];
                let bytes = ident.value.as_bytes();
                // Folded, then cut, as PostgreSQL does (ASCII: any byte is a
                // character boundary).
                let bytes = &bytes[..bytes.len().min(MAX_IDENT_BYTES)];
                for (out, byte) in folded.iter_mut().zip(bytes) {
                    *out = byte.to_ascii_lowercase();
                }
                // ASCII folded is still UTF-8.
                std::str::from_utf8(&folded[..bytes.len()])
                    .ok()
                    .and_then(KeyBuf::label)
            }
            Some('"') => KeyBuf::label(&ident.value),
            _ => None,
        }
    }

    /// The label of an object name of one or two parts: its table name. The
    /// schema is checked like the table name, then dropped.
    fn from_name(name: &ObjectName) -> Option<KeyBuf> {
        match name.0.as_slice() {
            [table] => KeyBuf::from_ident(table.as_ident()?),
            [schema, table] => {
                KeyBuf::from_ident(schema.as_ident()?)?;
                KeyBuf::from_ident(table.as_ident()?)
            }
            _ => None,
        }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).into_owned()
    }
}

// ---------------------------------------------------------------------------
// The hub: slots and counters
// ---------------------------------------------------------------------------

/// Client-chosen text (a table or application name) for a log line: `\`,
/// `"`, control characters and Unicode line and bidi controls are escaped
/// (`\n`, `\u{1b}`), so it can't end, forge or disguise a line. Distinct
/// texts stay distinct.
pub fn log_escape(text: &str) -> Cow<'_, str> {
    fn escaped(c: char) -> bool {
        c == '\\'
            || c == '"'
            || c.is_control()
            || matches!(
                c,
                '\u{061c}' | '\u{2028}' | '\u{2029}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
            )
    }
    if !text.chars().any(escaped) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\\' | '"' => {
                out.push('\\');
                out.push(c);
            }
            c if escaped(c) => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// One label's counters: one column per `IdentityState`, and (for slots) the
/// states already logged, as bits `1 << column`.
#[derive(Debug, Default)]
struct Row {
    counts: [AtomicU64; IdentityState::COUNT],
    seen: AtomicU8,
}

impl Row {
    fn add(&self, identity: IdentityState) {
        self.counts[identity.column()].fetch_add(1, Ordering::Relaxed);
    }

    fn read(&self) -> [u64; IdentityState::COUNT] {
        std::array::from_fn(|column| self.counts[column].load(Ordering::Relaxed))
    }
}

type Chunk = [Row; CHUNK_SLOTS];

/// One published registry: the names seen so far, append-only, and their
/// counters. Replaced whole only when the hub is republished or retired.
///
/// Storage is allocated as names arrive, so a hub that never counts a name
/// holds none: chunk `n` holds slots `n * CHUNK_SLOTS..`, is allocated under
/// the write lock with the first name it holds, and is never moved or freed
/// while the bank lives, so counting under the read lock never races a
/// reallocation. Every slot below `names.len()` has its chunk.
#[derive(Debug, Default)]
pub struct Bank {
    epoch: u32,
    active: bool,
    /// Labels, by slot.
    names: Vec<Box<str>>,
    /// Slot, by label.
    index: HashMap<Box<[u8]>, u16>,
    chunks: [Option<Box<Chunk>>; CHUNKS],
    fixed: [Row; FIXED_LABELS.len()],
}

// Inline in the hub: the fixed rows and the chunk pointers. Names, index and
// chunks are on the heap, allocated as names arrive.
const _: () = assert!(std::mem::size_of::<Bank>() <= 384);

impl Bank {
    fn live(epoch: u32) -> Bank {
        Bank {
            epoch,
            active: true,
            ..Bank::default()
        }
    }

    fn valid(&self) -> bool {
        self.active && self.epoch != 0
    }

    /// The counters of `slot`, once its name is in.
    fn row(&self, slot: usize) -> Option<&Row> {
        if slot >= self.names.len() {
            return None;
        }
        let chunk = self.chunks.get(slot / CHUNK_SLOTS)?.as_deref()?;
        chunk.get(slot % CHUNK_SLOTS)
    }

    /// Take the next slot for `key` (the caller checked it fits).
    fn insert(&mut self, key: &KeyBuf) -> u16 {
        let slot = self.names.len();
        if self.names.capacity() == 0 {
            self.names.reserve_exact(CHUNK_SLOTS);
            self.index.reserve(CHUNK_SLOTS);
        }
        if let Some(chunk @ None) = self.chunks.get_mut(slot / CHUNK_SLOTS) {
            *chunk = Some(Box::new(std::array::from_fn(|_| Row::default())));
        }
        self.names.push(key.text().into_boxed_str());
        self.index.insert(key.as_bytes().into(), slot as u16);
        slot as u16
    }

    #[cfg(test)]
    fn chunks_allocated(&self) -> usize {
        self.chunks.iter().filter(|chunk| chunk.is_some()).count()
    }
}

/// Bounded memory for the overflow log: the names since the last line.
#[derive(Debug)]
struct OverflowLog {
    names: [KeyBuf; OVERFLOW_LOG_NAMES],
    len: usize,
    /// Sightings past `names`.
    more: u64,
    last: Option<Instant>,
}

impl std::fmt::Debug for KeyBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_bytes()))
    }
}

/// What a classification had to report, logged once its locks are released.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Notes {
    /// The hub's name count when it reached `WARN_REGISTRY` or `MAX_REGISTRY`.
    pub capacity: Vec<usize>,
    /// The overflow line, if one was due.
    pub overflow: Option<String>,
}

/// Who sent an admitted statement, for the callers log.
#[derive(Clone, Copy, Debug)]
pub struct Caller<'a> {
    pub identity: IdentityState,
    pub application_name: &'a str,
    pub address: Option<std::net::SocketAddr>,
}

impl Caller<'static> {
    /// A caller with no client details (tests, internal callers).
    pub fn anonymous(identity: IdentityState) -> Caller<'static> {
        Caller {
            identity,
            application_name: "",
            address: None,
        }
    }
}

/// A pool lineage's relation slots and counters, shared (through the
/// observer's counters) with the pool a reload replaced.
#[derive(Debug)]
pub struct RelationHub {
    pool: String,
    user: String,
    bank: RwLock<Bank>,
    /// Allocated at the first overflow.
    overflow: Mutex<Option<Box<OverflowLog>>>,
}

impl Default for RelationHub {
    fn default() -> RelationHub {
        RelationHub::new("", "")
    }
}

impl RelationHub {
    pub fn new(pool: &str, user: &str) -> RelationHub {
        RelationHub {
            pool: pool.to_string(),
            user: user.to_string(),
            bank: RwLock::new(Bank::default()),
            overflow: Mutex::new(None),
        }
    }

    fn live(&self) -> bool {
        self.bank.read().valid()
    }

    /// The snapshot of a statement's labels: their slots, assigning new names
    /// first-seen. The write lock is taken only when a name is new and fits.
    pub fn resolve(
        &self,
        keys: &[KeyBuf],
        flags: u8,
        now: impl FnOnce() -> Instant,
    ) -> RelationSnap {
        let (snap, notes) = self.resolve_noting(keys, flags, now);
        for count in notes.capacity {
            warn!(
                "[pool: {}][user: {}] SQL observer relation names: {} of {}; names past {} count as other",
                self.pool, self.user, count, MAX_REGISTRY, MAX_REGISTRY
            );
        }
        if let Some(line) = notes.overflow {
            warn!("{}", line);
        }
        snap
    }

    /// `resolve`, returning what it would log.
    pub fn resolve_noting(
        &self,
        keys: &[KeyBuf],
        flags: u8,
        now: impl FnOnce() -> Instant,
    ) -> (RelationSnap, Notes) {
        let mut notes = Notes::default();
        let mut overflowed = [false; MAX_KEYS];
        let snap = 'resolved: {
            {
                let bank = self.bank.read();
                if !bank.valid() {
                    return (RelationSnap::UNKNOWN, notes);
                }
                let mut snap = RelationSnap {
                    epoch: bank.epoch,
                    slots: [0; MAX_KEYS],
                    len: 0,
                    flags,
                };
                let mut complete = true;
                for (i, key) in keys.iter().enumerate() {
                    match bank.index.get(key.as_bytes()) {
                        Some(&slot) => {
                            snap.slots[snap.len as usize] = slot;
                            snap.len += 1;
                        }
                        None if bank.names.len() >= MAX_REGISTRY => {
                            snap.flags |= FLAG_OTHER;
                            overflowed[i] = true;
                        }
                        None => {
                            complete = false;
                            break;
                        }
                    }
                }
                if complete {
                    break 'resolved snap;
                }
            }
            // A new name: resolve all of them again under the write lock, in
            // case a republication changed the epoch in between.
            overflowed = [false; MAX_KEYS];
            let mut bank = self.bank.write();
            if !bank.valid() {
                return (RelationSnap::UNKNOWN, notes);
            }
            let mut snap = RelationSnap {
                epoch: bank.epoch,
                slots: [0; MAX_KEYS],
                len: 0,
                flags,
            };
            for (i, key) in keys.iter().enumerate() {
                let slot = match bank.index.get(key.as_bytes()) {
                    Some(&slot) => slot,
                    None if bank.names.len() >= MAX_REGISTRY => {
                        snap.flags |= FLAG_OTHER;
                        overflowed[i] = true;
                        continue;
                    }
                    None => {
                        let slot = bank.insert(key);
                        let count = bank.names.len();
                        if count == WARN_REGISTRY || count == MAX_REGISTRY {
                            notes.capacity.push(count);
                        }
                        slot
                    }
                };
                snap.slots[snap.len as usize] = slot;
                snap.len += 1;
            }
            snap
        };
        if overflowed.iter().any(|o| *o) {
            let names = keys
                .iter()
                .zip(overflowed)
                .filter_map(|(key, o)| o.then_some(key));
            notes.overflow = self.note_overflow(names, now());
        }
        (snap, notes)
    }

    /// Remember names that overflowed; the line to log if one is due. Logging
    /// only, so a contended log is skipped rather than waited for.
    fn note_overflow<'k>(
        &self,
        names: impl Iterator<Item = &'k KeyBuf>,
        now: Instant,
    ) -> Option<String> {
        let mut log = self.overflow.try_lock()?;
        let log = log.get_or_insert_with(|| {
            Box::new(OverflowLog {
                names: [KeyBuf::EMPTY; OVERFLOW_LOG_NAMES],
                len: 0,
                more: 0,
                last: None,
            })
        });
        for name in names {
            if log.names[..log.len].contains(name) {
                continue;
            }
            if log.len < OVERFLOW_LOG_NAMES {
                let at = log.len;
                log.names[at] = *name;
                log.len += 1;
            } else {
                log.more += 1;
            }
        }
        let due = log.last.map_or(true, |last| {
            now.saturating_duration_since(last) >= OVERFLOW_LOG_INTERVAL
        });
        if !due || log.len == 0 {
            return None;
        }
        let listed: Vec<String> = log.names[..log.len]
            .iter()
            .map(|name| log_escape(&name.text()).into_owned())
            .collect();
        let more = if log.more > 0 {
            format!(", and {} more sightings", log.more)
        } else {
            String::new()
        };
        let line = format!(
            "[pool: {}][user: {}] SQL observer relation names are full ({}), new names count as other; names seen since the last such warning include: {}{}",
            self.pool,
            self.user,
            MAX_REGISTRY,
            listed.join(", "),
            more
        );
        log.len = 0;
        log.more = 0;
        log.last = Some(now);
        Some(line)
    }

    /// Count a statement's relations under the caller's identity, and log
    /// the first sighting of each relation with each identity state but
    /// `present` (at most `MAX_REGISTRY` × 3 lines per publication).
    pub fn admit(&self, snap: RelationSnap, caller: Caller<'_>) {
        for relation in self.admit_noting(snap, caller.identity, || ()) {
            info!(target: CALLERS_LOG_TARGET, "{}", self.callers_line(&relation, &caller));
        }
    }

    /// The callers log line for `relation`'s first sighting with the caller's
    /// identity state. The relation and application name come from the
    /// client, so both are quoted and escaped.
    fn callers_line(&self, relation: &str, caller: &Caller<'_>) -> String {
        format!(
            "[pool: {}][user: {}] relation \"{}\" first counted with identity {}: application_name \"{}\", client {}",
            self.pool,
            self.user,
            log_escape(relation),
            caller.identity.as_str(),
            log_escape(caller.application_name),
            caller
                .address
                .map_or_else(|| "unknown".to_string(), |address| address.to_string()),
        )
    }

    /// `admit`, atomically with the epoch check, returning the relations first
    /// sighted with this identity state. `during` runs between the check and
    /// the update, with the read guard held (for tests).
    pub fn admit_noting(
        &self,
        snap: RelationSnap,
        identity: IdentityState,
        during: impl FnOnce(),
    ) -> Vec<Box<str>> {
        let mut sighted = Vec::new();
        let bank = self.bank.read();
        during();
        if !bank.active {
            return sighted;
        }
        if !bank.valid() || snap.epoch != bank.epoch {
            bank.fixed[UNKNOWN].add(identity);
            return sighted;
        }
        let bit = 1u8 << identity.column();
        for &slot in snap.slots() {
            let slot = slot as usize;
            let Some(row) = bank.row(slot) else {
                continue;
            };
            row.add(identity);
            if identity != IdentityState::Present
                && row.seen.load(Ordering::Relaxed) & bit == 0
                && row.seen.fetch_or(bit, Ordering::Relaxed) & bit == 0
            {
                sighted.push(bank.names[slot].clone());
            }
        }
        for (flag, row) in [
            (FLAG_OTHER, OTHER),
            (FLAG_UNKNOWN, UNKNOWN),
            (FLAG_NO_EXPLICIT, NO_EXPLICIT),
        ] {
            if snap.flags & flag != 0 {
                bank.fixed[row].add(identity);
            }
        }
        sighted
    }

    /// Labels and values by `IdentityState` column: names by slot, then the
    /// fixed labels. Empty while the hub isn't published.
    pub fn samples(&self) -> Vec<(String, [u64; IdentityState::COUNT])> {
        let bank = self.bank.read();
        if !bank.active {
            return Vec::new();
        }
        let mut samples: Vec<_> = bank
            .names
            .iter()
            .enumerate()
            .map(|(slot, name)| {
                let counts = bank.row(slot).map_or([0; IdentityState::COUNT], Row::read);
                (name.to_string(), counts)
            })
            .collect();
        for (label, row) in FIXED_LABELS.iter().zip(&bank.fixed) {
            samples.push((label.to_string(), row.read()));
        }
        samples
    }

    /// The live epoch and names by slot (tests).
    pub fn current(&self) -> (u32, bool, Vec<String>) {
        let bank = self.bank.read();
        (
            bank.epoch,
            bank.active,
            bank.names.iter().map(|name| name.to_string()).collect(),
        )
    }

    #[cfg(test)]
    pub fn raw_bank(&self) -> &RwLock<Bank> {
        &self.bank
    }
}

/// Serializes hub publication and the metrics export, and issues epochs: an
/// epoch is never reused for another hub or another publication of one.
#[derive(Debug)]
pub struct Gate {
    last_epoch: u32,
}

impl Gate {
    pub const fn new() -> Gate {
        Gate { last_epoch: 0 }
    }

    /// A gate that has issued `last_epoch` (tests).
    pub const fn seeded(last_epoch: u32) -> Gate {
        Gate { last_epoch }
    }

    /// A fresh epoch, or 0 when they ran out (the hub then counts unknown only).
    fn next(&mut self) -> u32 {
        match self.last_epoch.checked_add(1) {
            Some(epoch) => {
                self.last_epoch = epoch;
                epoch
            }
            None => 0,
        }
    }

    /// Publish a hub: a published one keeps its epoch, slots and counts; one
    /// that isn't starts empty, with a fresh epoch.
    pub fn install(&mut self, hub: &RelationHub) {
        let mut bank = hub.bank.write();
        if bank.active {
            return;
        }
        *bank = Bank::live(self.next());
    }

    fn retire(hub: &RelationHub) {
        *hub.bank.write() = Bank::default();
    }

    /// Install every candidate's hub and retire the published hubs no
    /// candidate uses. The caller publishes the candidate pools before
    /// releasing the gate.
    pub fn publish(&mut self, current: &[&RelationHub], candidate: &[&RelationHub]) {
        for hub in candidate {
            self.install(hub);
        }
        for old in current {
            if !candidate.iter().any(|hub| std::ptr::eq(*hub, *old)) {
                Gate::retire(old);
            }
        }
    }
}

impl Default for Gate {
    fn default() -> Gate {
        Gate::new()
    }
}

/// The process-wide gate.
pub static GATE: Lazy<Mutex<Gate>> = Lazy::new(|| Mutex::new(Gate::new()));

// ---------------------------------------------------------------------------
// Classifier
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct AliasSlot {
    key: KeyBuf,
    /// Address of the CTE's body query, only compared for equality.
    body: usize,
    visible: bool,
}

const _: () = assert!(
    MAX_KEYS * std::mem::size_of::<KeyBuf>()
        + MAX_ALIASES * std::mem::size_of::<AliasSlot>()
        + MAX_SCOPES * std::mem::size_of::<u16>()
        + std::mem::size_of::<Classifier>()
        <= SCRATCH_BUDGET
);

/// Reusable classifier scratch: one per thread, see `classify`.
pub struct Classifier {
    keys: Box<[KeyBuf]>,
    key_len: usize,
    aliases: Box<[AliasSlot]>,
    alias_len: usize,
    frames: Box<[u16]>,
    frame_len: usize,
    failed: bool,
    saw_external: bool,
    flags: u8,
    pending_target: usize,
    skip_name: usize,
    expr_depth: usize,
}

fn filled<T: Copy>(len: usize, value: T) -> Box<[T]> {
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(value);
    }
    items.into_boxed_slice()
}

impl Classifier {
    pub fn new() -> Classifier {
        Classifier {
            keys: filled(MAX_KEYS, KeyBuf::EMPTY),
            key_len: 0,
            aliases: filled(
                MAX_ALIASES,
                AliasSlot {
                    key: KeyBuf::EMPTY,
                    body: 0,
                    visible: false,
                },
            ),
            alias_len: 0,
            frames: filled(MAX_SCOPES, 0u16),
            frame_len: 0,
            failed: false,
            saw_external: false,
            flags: 0,
            pending_target: 0,
            skip_name: 0,
            expr_depth: 0,
        }
    }

    /// Classify one original statement, and resolve its labels to the hub's
    /// slots.
    pub fn classify(&mut self, hub: &RelationHub, statement: &Statement) -> RelationSnap {
        if !hub.live() {
            return RelationSnap::UNKNOWN;
        }
        self.key_len = 0;
        self.alias_len = 0;
        self.frame_len = 0;
        self.failed = false;
        self.saw_external = false;
        self.flags = 0;
        self.pending_target = 0;
        self.skip_name = 0;
        self.expr_depth = 0;
        let mut run = Run { scratch: self };
        let _ = statement.visit(&mut run);
        let scratch = run.scratch;
        if scratch.failed || scratch.pending_target != 0 || scratch.skip_name != 0 {
            return RelationSnap::UNKNOWN;
        }
        let flags = if scratch.saw_external {
            scratch.flags
        } else {
            FLAG_NO_EXPLICIT
        };
        hub.resolve(&scratch.keys[..scratch.key_len], flags, Instant::now)
    }
}

impl Default for Classifier {
    fn default() -> Classifier {
        Classifier::new()
    }
}

thread_local! {
    /// The classifier scratch of this thread, allocated at its first
    /// classification. Sharing it between the clients a worker thread runs is
    /// sound because `Classifier::classify` is synchronous (no client can be
    /// switched in while it runs) and resets every field it reads before it
    /// reads it, so nothing one statement leaves behind reaches the next.
    static SCRATCH: RefCell<Option<Box<Classifier>>> = const { RefCell::new(None) };
}

/// Classify one original statement with this thread's scratch.
pub fn classify(hub: &RelationHub, statement: &Statement) -> RelationSnap {
    let shared = SCRATCH.try_with(|cell| {
        let mut scratch = cell.try_borrow_mut().ok()?;
        Some(
            scratch
                .get_or_insert_with(|| Box::new(Classifier::new()))
                .classify(hub, statement),
        )
    });
    match shared {
        Ok(Some(snap)) => snap,
        // Only while the thread is torn down, or if classification ever
        // reentered itself: a scratch of its own keeps the result the same.
        _ => Classifier::new().classify(hub, statement),
    }
}

/// The address of this thread's scratch, if it has one.
#[cfg(test)]
fn thread_scratch() -> Option<usize> {
    SCRATCH.with(|cell| cell.borrow().as_deref().map(addr))
}

struct Run<'a> {
    scratch: &'a mut Classifier,
}

fn addr<T>(value: &T) -> usize {
    value as *const T as usize
}

impl Run<'_> {
    fn fail(&mut self) -> ControlFlow<()> {
        self.scratch.failed = true;
        ControlFlow::Break(())
    }

    /// A visible CTE with this (unqualified) key.
    fn cte_visible(&self, key: &KeyBuf) -> bool {
        let s = &*self.scratch;
        s.aliases[..s.alias_len]
            .iter()
            .rev()
            .any(|slot| slot.visible && slot.key == *key)
    }

    /// Keep a relation's label, once per statement.
    fn add_relation(&mut self, key: &KeyBuf) -> ControlFlow<()> {
        let s = &mut *self.scratch;
        s.saw_external = true;
        if s.keys[..s.key_len].contains(key) {
            return ControlFlow::Continue(());
        }
        if s.key_len == MAX_KEYS {
            return self.fail();
        }
        s.keys[s.key_len] = *key;
        s.key_len += 1;
        ControlFlow::Continue(())
    }

    /// A relation named outside the visitor's callbacks (COPY, SELECT INTO).
    fn direct(&mut self, name: &ObjectName) -> ControlFlow<()> {
        match KeyBuf::from_name(name) {
            Some(key) => self.add_relation(&key),
            None => self.fail(),
        }
    }

    fn opaque(&mut self) {
        self.scratch.saw_external = true;
        self.scratch.flags |= FLAG_UNKNOWN;
    }

    fn set_pending(&mut self, name: &ObjectName) -> ControlFlow<()> {
        if self.scratch.pending_target != 0 {
            return self.fail();
        }
        self.scratch.pending_target = addr(name);
        ControlFlow::Continue(())
    }

    /// Only a TABLE command's names that read the same quoted or not are
    /// labeled.
    fn table_command(&mut self, table: &Table) -> ControlFlow<()> {
        let Some(name) = table.table_name.as_ref().map(|name| name.value.as_str()) else {
            return self.fail();
        };
        if !bare(name)
            || table
                .schema_name
                .as_ref()
                .is_some_and(|schema| !bare(&schema.value))
        {
            return self.fail();
        }
        let Some(key) = KeyBuf::label(name) else {
            return self.fail();
        };
        if table.schema_name.is_none() && self.cte_visible(&key) {
            return ControlFlow::Continue(());
        }
        self.add_relation(&key)
    }

    fn scan_body(&mut self, body: &SetExpr, depth: usize) -> ControlFlow<()> {
        if depth > MAX_SCOPES {
            return self.fail();
        }
        match body {
            SetExpr::Select(select) => {
                if !select.lateral_views.is_empty() {
                    return self.fail();
                }
                if let Some(into) = &select.into {
                    // PostgreSQL's SELECT INTO has one target, the new table.
                    return match into.targets.as_slice() {
                        [Expr::Identifier(name)] => {
                            self.direct(&ObjectName::from(vec![name.clone()]))
                        }
                        [Expr::CompoundIdentifier(name)] => {
                            self.direct(&ObjectName::from(name.clone()))
                        }
                        _ => self.fail(),
                    };
                }
                ControlFlow::Continue(())
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.scan_body(left, depth + 1)?;
                self.scan_body(right, depth + 1)
            }
            SetExpr::Table(table) => self.table_command(table),
            _ => ControlFlow::Continue(()),
        }
    }

    fn dml_target(&mut self, relation: &TableFactor) -> ControlFlow<()> {
        match relation {
            TableFactor::Table {
                name, args: None, ..
            } => self.set_pending(name),
            _ => self.fail(),
        }
    }
}

impl Visitor for Run<'_> {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
        match statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => self.set_pending(name),
                _ => self.fail(),
            },
            Statement::Update(Update { table, .. }) => {
                if !table.joins.is_empty() {
                    return self.fail();
                }
                self.dml_target(&table.relation)
            }
            Statement::Delete(Delete { tables, from, .. }) => {
                let (FromTable::WithFromKeyword(targets) | FromTable::WithoutKeyword(targets)) =
                    from;
                if !tables.is_empty() || targets.len() != 1 || !targets[0].joins.is_empty() {
                    return self.fail();
                }
                self.dml_target(&targets[0].relation)
            }
            Statement::Copy {
                source: CopySource::Table { table_name, .. },
                ..
            } => self.direct(table_name),
            Statement::Copy { .. }
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. }
            | Statement::Close { .. }
            | Statement::Deallocate { .. }
            | Statement::Discard { .. }
            // SET of a variable, ROLE, TIME ZONE or TRANSACTION; not SET NAMES
            // or SET SESSION AUTHORIZATION.
            | Statement::Set(
                Set::SingleAssignment { .. }
                | Set::ParenthesizedAssignments { .. }
                | Set::MultipleAssignments { .. }
                | Set::SetRole { .. }
                | Set::SetTimeZone { .. }
                | Set::SetTransaction { .. },
            )
            | Statement::ShowVariable { .. } => ControlFlow::Continue(()),
            _ => self.fail(),
        }
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::Table {
                name,
                args,
                with_hints,
                version,
                partitions,
                json_path,
                sample,
                index_directives,
                ..
            } => {
                if !with_hints.is_empty()
                    || version.is_some()
                    || !partitions.is_empty()
                    || json_path.is_some()
                    || sample.is_some()
                    || !index_directives.is_empty()
                {
                    return self.fail();
                }
                if args.is_some() {
                    self.scratch.skip_name = addr(name);
                    self.opaque();
                }
                ControlFlow::Continue(())
            }
            TableFactor::Derived { .. } | TableFactor::NestedJoin { .. } => {
                ControlFlow::Continue(())
            }
            TableFactor::TableFunction { .. }
            | TableFactor::Function { .. }
            | TableFactor::UNNEST { .. }
            | TableFactor::JsonTable { .. } => {
                self.opaque();
                ControlFlow::Continue(())
            }
            _ => self.fail(),
        }
    }

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<()> {
        let at = addr(name);
        if self.scratch.skip_name == at {
            self.scratch.skip_name = 0;
            return ControlFlow::Continue(());
        }
        let direct = self.scratch.pending_target == at;
        if direct {
            self.scratch.pending_target = 0;
        }
        let Some(key) = KeyBuf::from_name(name) else {
            return self.fail();
        };
        if !direct && name.0.len() == 1 && self.cte_visible(&key) {
            return ControlFlow::Continue(());
        }
        self.add_relation(&key)
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.scratch.frame_len == MAX_SCOPES {
            return self.fail();
        }
        let base = self.scratch.alias_len;
        self.scratch.frames[self.scratch.frame_len] = base as u16;
        self.scratch.frame_len += 1;
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let key = match KeyBuf::from_ident(&cte.alias.name) {
                    Some(key) if cte.from.is_none() => key,
                    _ => return self.fail(),
                };
                let s = &mut *self.scratch;
                if s.alias_len == MAX_ALIASES
                    || s.aliases[base..s.alias_len]
                        .iter()
                        .any(|slot| slot.key.as_bytes() == key.as_bytes())
                {
                    return self.fail();
                }
                s.aliases[s.alias_len] = AliasSlot {
                    key,
                    body: addr(&*cte.query),
                    visible: with.recursive,
                };
                s.alias_len += 1;
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        // The body sees every alias of its own WITH: they are all visible now.
        self.scan_body(&query.body, 0)?;
        let s = &mut *self.scratch;
        s.frame_len -= 1;
        let base = s.frames[s.frame_len] as usize;
        s.alias_len = base;
        let at = addr(query);
        if let Some(slot) = s.aliases[..base].iter_mut().find(|slot| slot.body == at) {
            slot.visible = true;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &sqlparser::ast::Expr) -> ControlFlow<()> {
        self.scratch.expr_depth += 1;
        if self.scratch.expr_depth > MAX_EXPR_DEPTH {
            return self.fail();
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &sqlparser::ast::Expr) -> ControlFlow<()> {
        self.scratch.expr_depth = self.scratch.expr_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::sync::{mpsc, Arc};

    const NOEXP: &str = "no_explicit_relation";

    fn live() -> (RelationHub, Gate) {
        let hub = RelationHub::new("db", "app");
        let mut gate = Gate::new();
        gate.install(&hub);
        (hub, gate)
    }

    fn parse(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn snap_for(hub: &RelationHub, sql: &str) -> RelationSnap {
        Classifier::new().classify(hub, &parse(sql)[0])
    }

    /// The labels a snapshot would count under, sorted.
    fn labels(hub: &RelationHub, snap: RelationSnap) -> Vec<String> {
        let (_, _, names) = hub.current();
        let mut out: Vec<String> = snap
            .slots()
            .iter()
            .map(|&slot| names[slot as usize].clone())
            .collect();
        for (flag, label) in [
            (FLAG_OTHER, "other"),
            (FLAG_UNKNOWN, "unknown"),
            (FLAG_NO_EXPLICIT, NOEXP),
        ] {
            if snap.flags & flag != 0 {
                out.push(label.to_string());
            }
        }
        out.sort();
        out
    }

    fn key(label: &str) -> KeyBuf {
        let mut key = KeyBuf::EMPTY;
        for &byte in label.as_bytes() {
            assert!(key.push_byte(byte));
        }
        key
    }

    fn count(hub: &RelationHub, label: &str) -> [u64; IdentityState::COUNT] {
        hub.samples()
            .into_iter()
            .find(|(name, _)| name == label)
            .map(|(_, values)| values)
            .unwrap_or_else(|| panic!("no {label}"))
    }

    const PRESENT: IdentityState = IdentityState::Present;
    const ABSENT: IdentityState = IdentityState::Absent;

    #[test]
    fn classifier_table() {
        let (hub, _gate) = live();
        let cases: &[(&str, &str, &[&str])] = &[
            (
                "qualified_ascii_fold",
                "SELECT * FROM PUBLIC.ORDERS",
                &["orders"],
            ),
            (
                "quoted_case_kept",
                "SELECT * FROM \"Sales\".\"Orders\"",
                &["\"Orders\""],
            ),
            (
                "quoted_case_kept",
                "SELECT * FROM \"public\".\"Orders\"",
                &["\"Orders\""],
            ),
            (
                "quoted_lower_is_bare",
                "SELECT * FROM \"public\".\"orders\"",
                &["orders"],
            ),
            (
                "quoted_space",
                "SELECT * FROM \"My Table\"",
                &["\"My Table\""],
            ),
            (
                "doubled_quote",
                "SELECT * FROM a.\"we\"\"ird\"",
                &["\"we\"\"ird\""],
            ),
            ("dollar_digit", "SELECT * FROM t$1, _x9", &["_x9", "t$1"]),
            (
                "fixed_label_names",
                "SELECT * FROM other, public.unknown",
                &["\"other\"", "\"unknown\""],
            ),
            (
                "schema_dropped",
                "SELECT * FROM public.unconfigured",
                &["unconfigured"],
            ),
            ("unqualified", "SELECT * FROM orders", &["orders"]),
            ("arity", "SELECT * FROM db.public.orders", &["unknown"]),
            (
                "schemas_share_the_name",
                "SELECT * FROM public.orders JOIN archive.orders USING (id), ORDERS",
                &["orders"],
            ),
            (
                "self_join_dedup",
                "SELECT * FROM public.orders a JOIN public.orders b ON a.id = b.id \
                 WHERE EXISTS (SELECT 1 FROM public.orders c)",
                &["orders"],
            ),
            (
                "multi",
                "SELECT * FROM public.orders JOIN public.items USING (id)",
                &["items", "orders"],
            ),
            (
                "cte_body_not_alias",
                "WITH x AS (SELECT * FROM public.orders) SELECT * FROM x",
                &["orders"],
            ),
            (
                "cte_shadow_constant",
                "WITH orders AS (SELECT 1) SELECT * FROM orders",
                &[NOEXP],
            ),
            (
                "cte_shadow_quoted_same_value",
                "WITH orders AS (SELECT 1) SELECT * FROM \"orders\"",
                &[NOEXP],
            ),
            (
                "cte_qualified_is_a_table",
                "WITH orders AS (SELECT 1) SELECT * FROM public.orders",
                &["orders"],
            ),
            (
                "cte_preceding_sibling",
                "WITH x AS (SELECT * FROM public.orders), y AS (SELECT * FROM x) SELECT * FROM y",
                &["orders"],
            ),
            (
                "cte_nonrecursive_self_not_visible",
                "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders",
                &["orders"],
            ),
            (
                "cte_later_sibling_not_visible",
                "WITH x AS (SELECT * FROM y), y AS (SELECT 1) SELECT * FROM x",
                &["y"],
            ),
            (
                "cte_recursive",
                "WITH RECURSIVE r AS (SELECT id FROM public.orders UNION ALL \
                 SELECT id FROM r) SELECT * FROM r",
                &["orders"],
            ),
            (
                "cte_nested_shadow",
                "WITH x AS (SELECT * FROM public.orders) \
                 SELECT * FROM (WITH x AS (SELECT * FROM public.items) SELECT * FROM x) s, x",
                &["items", "orders"],
            ),
            (
                "cte_is_not_dml_target_update",
                "WITH orders AS (SELECT 1 AS id) UPDATE orders SET id = 1",
                &["orders"],
            ),
            (
                "cte_is_not_dml_target_insert",
                "WITH orders AS (SELECT 1 AS id) INSERT INTO orders SELECT * FROM orders",
                &["orders"],
            ),
            ("table_safe", "(TABLE public.orders)", &["orders"]),
            (
                "table_quote_loss",
                "(TABLE \"Sales\".\"Orders\")",
                &["unknown"],
            ),
            ("table_quoted", "(TABLE \"public\".\"orders\")", &["orders"]),
            ("table_upper", "(TABLE Public.Orders)", &["unknown"]),
            ("table_cte", "WITH x AS (SELECT 1) TABLE x", &[NOEXP]),
            (
                "table_unqualified",
                "WITH y AS (SELECT 1) TABLE orders",
                &["orders"],
            ),
            (
                "fixed_label_names",
                "SELECT * FROM other, s.unknown, no_explicit_relation",
                &["\"other\"", "\"unknown\"", "\"no_explicit_relation\""],
            ),
            ("copy_from", "COPY public.orders FROM '/tmp/x'", &["orders"]),
            ("copy_to", "COPY public.orders (id) TO STDOUT", &["orders"]),
            (
                "copy_query",
                "COPY (SELECT * FROM public.orders JOIN public.items USING (id)) TO STDOUT",
                &["items", "orders"],
            ),
            (
                "function_table",
                "SELECT * FROM public.orders(1)",
                &["unknown"],
            ),
            (
                "function_table_joined",
                "SELECT * FROM public.orders, generate_series(1, 3)",
                &["orders", "unknown"],
            ),
            ("scalar_function", "SELECT public.f()", &[NOEXP]),
            (
                "scalar_subquery",
                "SELECT public.f((SELECT id FROM public.orders LIMIT 1))",
                &["orders"],
            ),
            (
                "view_name_only",
                "SELECT * FROM public.order_view",
                &["order_view"],
            ),
            (
                "insert_select",
                "INSERT INTO public.orders SELECT * FROM public.items",
                &["items", "orders"],
            ),
            (
                "update_from",
                "UPDATE public.orders SET id = 1 FROM public.items WHERE true",
                &["items", "orders"],
            ),
            (
                "delete_using",
                "DELETE FROM public.orders USING public.items WHERE true",
                &["items", "orders"],
            ),
            ("select_into", "SELECT 1 INTO public.orders", &["orders"]),
            ("constant", "SELECT 1", &[NOEXP]),
            ("values", "VALUES (1)", &[NOEXP]),
            ("begin", "BEGIN", &[NOEXP]),
            ("commit", "COMMIT", &[NOEXP]),
            ("savepoint", "SAVEPOINT s", &[NOEXP]),
            ("set", "SET statement_timeout = 1", &[NOEXP]),
            ("show", "SHOW work_mem", &[NOEXP]),
            ("ddl", "CREATE TABLE public.orders (id int)", &["unknown"]),
            ("drop", "DROP TABLE public.orders", &["unknown"]),
            (
                "explain",
                "EXPLAIN SELECT * FROM public.orders",
                &["unknown"],
            ),
            (
                "prepare",
                "PREPARE p AS SELECT * FROM public.orders",
                &["unknown"],
            ),
            ("execute", "EXECUTE p", &["unknown"]),
            ("truncate", "TRUNCATE public.orders", &["unknown"]),
        ];
        for (name, sql, want) in cases {
            let snap = snap_for(&hub, sql);
            let mut want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
            want.sort();
            assert_eq!(labels(&hub, snap), want, "{name}: {sql}");
        }
    }

    #[test]
    fn labels_are_injective_and_never_fixed() {
        for (value, label) in [
            ("orders", "orders"),
            ("Orders", "\"Orders\""),
            ("other", "\"other\""),
            ("no_explicit_relation", "\"no_explicit_relation\""),
            ("a\"b", "\"a\"\"b\""),
            ("9lives", "\"9lives\""),
            ("straße", "\"straße\""),
        ] {
            assert_eq!(KeyBuf::label(value).unwrap().as_bytes(), label.as_bytes());
        }
        assert!(KeyBuf::label("").is_none());
        assert!(KeyBuf::label("a\0b").is_none());
    }

    /// Every string over a small alphabet up to 5 characters, plus long runs
    /// of each pair of characters around the byte and character caps.
    fn label_inputs() -> Vec<String> {
        const ALPHABET: [char; 7] = ['a', 'Z', '_', '1', '"', 'é', '\0'];
        let mut inputs = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..5 {
            frontier = frontier
                .iter()
                .flat_map(|prefix| ALPHABET.iter().map(move |&c| format!("{prefix}{c}")))
                .collect();
            inputs.extend(frontier.iter().cloned());
        }
        for &first in &ALPHABET {
            for &second in &ALPHABET {
                for len in [31, 32, 62, 63, 64, 65, 70, 130] {
                    for split in [0, 1, len / 2, len - 1, len] {
                        let run = std::iter::repeat(first).take(split);
                        inputs.push(
                            run.chain(std::iter::repeat(second).take(len - split))
                                .collect(),
                        );
                    }
                }
            }
        }
        inputs.extend(FIXED_LABELS.iter().map(|label| label.to_string()));
        inputs
    }

    #[test]
    fn every_label_is_capped_quoted_unless_bare_and_never_fixed() {
        for value in label_inputs() {
            let cut = clip(&value, MAX_IDENT_BYTES);
            let Some(key) = KeyBuf::label(&value) else {
                assert!(cut.is_empty() || cut.contains('\0'), "{value:?}");
                continue;
            };
            let label = key.text();
            assert_eq!(label.as_bytes(), key.as_bytes(), "{value:?}");
            assert!(label.chars().count() <= MAX_LABEL_CHARS, "{value:?}");
            assert!(label.len() <= MAX_NAME_BYTES, "{value:?}");
            assert!(!FIXED_LABELS.contains(&label.as_str()), "{value:?}");
            if bare(cut) && !FIXED_LABELS.contains(&cut) {
                assert_eq!(label, cut, "{value:?}");
                continue;
            }
            // Quoted: a prefix of the full quoted form, and all of it when
            // that fits the cap.
            let full = format!("\"{}\"", cut.replace('"', "\"\""));
            assert!(
                label.starts_with('"') && full.starts_with(&label),
                "{value:?}: {label}"
            );
            if full.chars().count() <= MAX_LABEL_CHARS {
                assert_eq!(label, full, "{value:?}");
            }
        }
    }

    fn label_text(value: &str) -> String {
        KeyBuf::label(value).unwrap().text()
    }

    #[test]
    fn long_names_are_cut_like_postgres_and_labels_capped() {
        let (hub, _gate) = live();
        let slot = |sql: &str| snap_for(&hub, sql).slots().to_vec();
        // Unquoted: folded, then cut to 63 bytes; a twin sharing them, or
        // another spelling, is the same name and slot.
        let long = "x".repeat(70);
        let twin = format!("{}{}", "x".repeat(63), "y".repeat(7));
        assert_eq!(slot(&format!("SELECT * FROM s.{long}")), [0]);
        assert_eq!(slot(&format!("SELECT * FROM {twin}")), [0]);
        assert_eq!(slot(&format!("SELECT * FROM {}", long.to_uppercase())), [0]);
        assert_eq!(slot(&format!("SELECT * FROM \"{}\"", "x".repeat(63))), [0]);
        assert_eq!(hub.current().2, ["x".repeat(63)]);
        // Quoted: cut to 63 bytes, then the label to 64 characters (its
        // closing quote).
        let quoted = "X".repeat(70);
        let quoted_twin = format!("{}{}", "X".repeat(63), "Z".repeat(7));
        assert_eq!(slot(&format!("SELECT * FROM \"{quoted}\"")), [1]);
        assert_eq!(slot(&format!("SELECT * FROM \"{quoted_twin}\"")), [1]);
        assert_eq!(slot(&format!("SELECT * FROM \"{}\"", "X".repeat(62))), [2]);
        let names = hub.current().2;
        assert_eq!(names[1], format!("\"{}", "X".repeat(63)));
        assert_eq!(names[2], format!("\"{}\"", "X".repeat(62)));
        assert!(names
            .iter()
            .all(|name| name.chars().count() <= MAX_LABEL_CHARS));
        // A name of quotes only: still at most 64 characters.
        assert_eq!(label_text(&"\"".repeat(70)), "\"".repeat(MAX_LABEL_CHARS));
    }

    #[test]
    fn multibyte_names_are_cut_on_a_character_boundary() {
        // 80 bytes; byte 63 is inside the 32nd é, so 31 are kept.
        let label = label_text(&"é".repeat(40));
        assert_eq!(label, format!("\"{}\"", "é".repeat(31)));
        assert_eq!(clip("aé", 2), "a");
        assert_eq!(clip("aé", 3), "aé");
    }

    #[test]
    fn short_names_keep_their_labels() {
        for (value, label) in [
            ("orders", "orders"),
            ("Orders", "\"Orders\""),
            ("a\"b", "\"a\"\"b\""),
            (&*"x".repeat(63), &*"x".repeat(63)),
            (&*"X".repeat(62), &*format!("\"{}\"", "X".repeat(62))),
        ] {
            assert_eq!(label_text(value), label);
        }
    }

    #[test]
    fn sixteen_seventeen_and_duplicates() {
        let (hub, _gate) = live();
        let from = |n: usize| {
            let list: Vec<String> = (0..n).map(|i| format!("s.t{i}")).collect();
            format!("SELECT * FROM {}", list.join(", "))
        };
        let snap = snap_for(&hub, &from(16));
        assert_eq!((snap.len, snap.flags), (16, 0));
        let snap = snap_for(&hub, &from(17));
        assert_eq!(snap, RelationSnap::UNKNOWN);
        // 17 references to one name, in two schemas.
        let many = [vec!["s.t0"; 9], vec!["r.t0"; 8]].concat().join(", ");
        let snap = snap_for(&hub, &format!("SELECT * FROM {many}"));
        assert_eq!((snap.slots(), snap.flags), (&[0u16][..], 0));
    }

    #[test]
    fn bounded_syntax_fallback() {
        // Deeply nested SQL needs more stack than a test thread has.
        std::thread::Builder::new()
            .stack_size(256 << 20)
            .spawn(bounded_syntax_fallback_body)
            .unwrap()
            .join()
            .unwrap();
    }

    fn bounded_syntax_fallback_body() {
        let (hub, _gate) = live();
        let unknown_only = |sql: &str| {
            assert_eq!(snap_for(&hub, sql), RelationSnap::UNKNOWN, "{sql}");
        };
        unknown_only("SELECT * FROM public.straße");
        unknown_only("SELECT * FROM straße.orders");
        // 65 CTE aliases.
        let ctes: Vec<String> = (0..65).map(|i| format!("c{i} AS (SELECT 1)")).collect();
        unknown_only(&format!("WITH {} SELECT * FROM c0", ctes.join(", ")));
        // Duplicate alias.
        unknown_only("WITH x AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x");
        // Excess scope depth.
        let mut sql = "SELECT 1".to_string();
        for _ in 0..40 {
            sql = format!("SELECT * FROM ({sql}) s");
        }
        let statements = Parser::new(&PostgreSqlDialect {})
            .with_recursion_limit(500)
            .try_with_sql(&sql)
            .unwrap()
            .parse_statements()
            .unwrap();
        assert_eq!(
            Classifier::new().classify(&hub, &statements[0]),
            RelationSnap::UNKNOWN
        );
        // Excess expression depth.
        let mut expr = "1".to_string();
        for _ in 0..70 {
            expr = format!("({expr} + 1)");
        }
        let statements = Parser::new(&PostgreSqlDialect {})
            .with_recursion_limit(2000)
            .try_with_sql(&format!("SELECT {expr}"))
            .unwrap()
            .parse_statements()
            .unwrap();
        assert_eq!(
            Classifier::new().classify(&hub, &statements[0]),
            RelationSnap::UNKNOWN
        );
        // Nothing that failed took a slot.
        assert!(hub.current().2.is_empty());
    }

    #[test]
    fn unsupported_table_sources_are_unknown_only() {
        let (hub, _gate) = live();
        // A table source with a hint is not supported: exactly unknown.
        let mut statements = parse("SELECT * FROM public.orders");
        let Statement::Query(query) = &mut statements[0] else {
            panic!("query expected");
        };
        let SetExpr::Select(select) = &mut *query.body else {
            panic!("select expected");
        };
        let TableFactor::Table { with_hints, .. } = &mut select.from[0].relation else {
            panic!("table expected");
        };
        with_hints.push(sqlparser::ast::Expr::value(sqlparser::ast::Value::Number(
            "1".into(),
            false,
        )));
        let snap = Classifier::new().classify(&hub, &statements[0]);
        assert_eq!(snap, RelationSnap::UNKNOWN);
        // A target that is not a plain table cannot be assigned.
        let mut statements = parse("DELETE FROM public.orders USING public.items");
        if let Statement::Delete(delete) = &mut statements[0] {
            delete.tables = vec![ObjectName::from(vec![Ident::new("x")])];
        }
        let snap = Classifier::new().classify(&hub, &statements[0]);
        assert_eq!(snap, RelationSnap::UNKNOWN);
        let snap = snap_for(&hub, "UPDATE public.orders SET id = 1");
        assert_eq!(labels(&hub, snap), vec!["orders".to_string()]);
    }

    #[test]
    fn classification_does_not_leak_between_statements() {
        let (hub, _gate) = live();
        let mut classifier = Classifier::new();
        let first = parse("WITH x AS (SELECT * FROM public.orders) SELECT * FROM x, public.items");
        let second = parse("SELECT * FROM x");
        let a = classifier.classify(&hub, &first[0]);
        let b = classifier.classify(&hub, &second[0]);
        assert_eq!(labels(&hub, a), vec!["items", "orders"]);
        assert_eq!(labels(&hub, b), vec!["x"]);
    }

    #[test]
    fn thread_scratch_is_reused_across_clients_without_leaking() {
        let (orders, _orders_gate) = live();
        let (items, _items_gate) = live();
        let failing = (0..=MAX_KEYS)
            .map(|i| format!("SELECT * FROM t{i}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        assert_eq!(
            classify(&orders, &parse(&failing)[0]),
            RelationSnap::UNKNOWN
        );
        let scratch = thread_scratch();
        assert!(scratch.is_some());
        let first = classify(
            &orders,
            &parse("WITH x AS (SELECT * FROM public.orders) SELECT * FROM x")[0],
        );
        let second = classify(&items, &parse("SELECT * FROM x")[0]);
        assert_eq!(labels(&orders, first), vec!["orders"]);
        assert_eq!(labels(&items, second), vec!["x"]);
        assert_eq!(thread_scratch(), scratch);

        let other = std::thread::spawn(|| {
            assert_eq!(thread_scratch(), None);
            let (hub, _gate) = live();
            let snap = classify(&hub, &parse("SELECT * FROM a")[0]);
            (labels(&hub, snap), thread_scratch())
        })
        .join()
        .expect("classifier thread");
        assert_eq!(other.0, vec!["a"]);
        assert!(other.1.is_some());
    }

    // --- Slots ---

    #[test]
    fn first_seen_slots_are_stable_across_republication() {
        let (hub, mut gate) = live();
        let epoch = hub.current().0;
        let ab = snap_for(&hub, "SELECT * FROM b JOIN a USING (id)");
        let ca = snap_for(&hub, "SELECT * FROM public.c, archive.a");
        assert_eq!(hub.current().2, vec!["b", "a", "c"]);
        assert_eq!((ab.slots(), ca.slots()), (&[0u16, 1][..], &[2u16, 1][..]));
        hub.admit_noting(ab, PRESENT, || ());
        // A reload republishes the same hub: same epoch, slots and counts.
        gate.publish(&[&hub], &[&hub]);
        assert_eq!(hub.current().0, epoch);
        assert_eq!(hub.current().2, vec!["b", "a", "c"]);
        hub.admit_noting(ab, ABSENT, || ());
        hub.admit_noting(ca, ABSENT, || ());
        assert_eq!(count(&hub, "a"), [1, 2, 0, 0]);
        assert_eq!(count(&hub, "b"), [1, 1, 0, 0]);
        assert_eq!(count(&hub, "c"), [0, 1, 0, 0]);
        // Known names resolve under the read lock to the same slots.
        assert_eq!(snap_for(&hub, "SELECT * FROM a, b"), {
            let mut snap = ab;
            snap.slots[..2].copy_from_slice(&[1, 0]);
            snap
        });
        // A new name is appended.
        assert_eq!(snap_for(&hub, "SELECT * FROM d").slots(), &[3]);
        // Labels: names by slot, then the fixed ones.
        let names: Vec<String> = hub.samples().into_iter().map(|(name, _)| name).collect();
        assert_eq!(names, ["b", "a", "c", "d", "other", "unknown", NOEXP]);
    }

    /// (chunks allocated, name capacity, index capacity, overflow log allocated).
    fn storage(hub: &RelationHub) -> (usize, usize, usize, bool) {
        let bank = hub.raw_bank().read();
        (
            bank.chunks_allocated(),
            bank.names.capacity(),
            bank.index.capacity(),
            hub.overflow.lock().is_some(),
        )
    }

    #[test]
    fn storage_is_allocated_as_names_arrive() {
        // Never published, or published and never counting: nothing.
        let hub = RelationHub::new("db", "app");
        assert_eq!(storage(&hub), (0, 0, 0, false));
        let (hub, mut gate) = live();
        hub.admit_noting(RelationSnap::UNKNOWN, ABSENT, || ());
        let (snap, _) = hub.resolve_noting(&[], FLAG_NO_EXPLICIT, Instant::now);
        hub.admit_noting(snap, ABSENT, || ());
        assert_eq!(storage(&hub), (0, 0, 0, false));
        assert_eq!(count(&hub, "unknown"), [0, 1, 0, 0]);
        // The first name: chunk 0, and room for 64 names.
        let names: Vec<KeyBuf> = (0..=CHUNK_SLOTS).map(|i| key(&format!("t{i}"))).collect();
        let (first, _) = hub.resolve_noting(&names[..1], 0, Instant::now);
        let (chunks, names_capacity, index_capacity, overflow) = storage(&hub);
        assert_eq!((chunks, overflow), (1, false));
        assert!(names_capacity == CHUNK_SLOTS && index_capacity >= CHUNK_SLOTS);
        hub.admit_noting(first, PRESENT, || ());
        let row = hub.raw_bank().read().row(0).map(|row| row as *const Row);
        // Up to the 64th name, still chunk 0; the 65th takes chunk 1.
        for name in &names[1..CHUNK_SLOTS] {
            hub.resolve_noting(std::slice::from_ref(name), 0, Instant::now);
        }
        assert_eq!(storage(&hub).0, 1);
        let (edge, _) = hub.resolve_noting(&names[CHUNK_SLOTS - 1..], 0, Instant::now);
        assert_eq!(edge.slots(), &[63, 64]);
        assert_eq!(storage(&hub).0, 2);
        // Counts and slots hold across the boundary and a reload; the
        // first chunk never moved.
        hub.admit_noting(edge, ABSENT, || ());
        hub.admit_noting(first, ABSENT, || ());
        gate.publish(&[&hub], &[&hub]);
        assert_eq!(
            hub.raw_bank().read().row(0).map(|row| row as *const Row),
            row
        );
        let (again, _) = hub.resolve_noting(&[names[CHUNK_SLOTS], names[0]], 0, Instant::now);
        assert_eq!(again.slots(), &[64, 0]);
        hub.admit_noting(again, ABSENT, || ());
        assert_eq!(count(&hub, "t0"), [1, 2, 0, 0]);
        assert_eq!(count(&hub, "t63"), [0, 1, 0, 0]);
        assert_eq!(count(&hub, "t64"), [0, 2, 0, 0]);
        assert_eq!(storage(&hub).0, 2);
        // At the cap, every chunk; overflow allocates the log, not a chunk.
        let rest: Vec<KeyBuf> = (CHUNK_SLOTS + 1..MAX_REGISTRY)
            .map(|i| key(&format!("t{i}")))
            .collect();
        for chunk in rest.chunks(MAX_KEYS) {
            hub.resolve_noting(chunk, 0, Instant::now);
        }
        assert_eq!(storage(&hub).0, CHUNKS);
        assert_eq!(CHUNKS, 16);
        let (late, _) = hub.resolve_noting(&[key("late")], 0, Instant::now);
        assert_eq!((late.len, late.flags), (0, FLAG_OTHER));
        assert_eq!((storage(&hub).0, storage(&hub).3), (CHUNKS, true));
        // Retired: the storage goes with the bank.
        gate.publish(&[&hub], &[]);
        assert_eq!(storage(&hub).0, 0);
    }

    #[test]
    fn cap_overflow_counts_other_and_warns_once() {
        let (hub, _gate) = live();
        let t0 = Instant::now();
        let mut capacity = Vec::new();
        // Up to the cap, sixteen names per statement.
        let names: Vec<KeyBuf> = (0..MAX_REGISTRY).map(|i| key(&format!("t{i}"))).collect();
        for chunk in names.chunks(MAX_KEYS) {
            let (snap, notes) = hub.resolve_noting(chunk, 0, || t0);
            assert_eq!((snap.len as usize, snap.flags), (chunk.len(), 0));
            assert_eq!(notes.overflow, None);
            capacity.extend(notes.capacity);
        }
        assert_eq!(capacity, vec![WARN_REGISTRY, MAX_REGISTRY]);
        assert_eq!(hub.current().2.len(), MAX_REGISTRY);
        // Known names still resolve; nothing warns again.
        let (snap, notes) = hub.resolve_noting(&names[998..], 0, || t0);
        assert_eq!(
            (snap.slots(), notes),
            (&[998u16, 999][..], Notes::default())
        );
        // Past the cap: other, logged at once, then at most once a minute.
        // A client-chosen name can't break the line.
        let forged = key("\"late0\n2026-11-09T00:00:00Z WARN forged \u{1b}[2J\"");
        let (snap, notes) = hub.resolve_noting(&[names[0], forged], 0, || t0);
        assert_eq!((snap.slots(), snap.flags), (&[0u16][..], FLAG_OTHER));
        let line = notes.overflow.unwrap();
        assert!(!line.chars().any(char::is_control), "{line:?}");
        assert!(
            line.contains("[pool: db][user: app]")
                && line
                    .ends_with(r#"include: \"late0\n2026-11-09T00:00:00Z WARN forged \u{1b}[2J\""#),
            "{line}"
        );
        assert!(notes.capacity.is_empty());
        hub.admit_noting(snap, ABSENT, || ());
        assert_eq!(count(&hub, "other"), [0, 1, 0, 0]);
        assert_eq!(count(&hub, "t0"), [0, 1, 0, 0]);
        // Within the minute: remembered (deduplicated, up to 16), not logged.
        let later: Vec<KeyBuf> = (1..=20).map(|i| key(&format!("late{i}"))).collect();
        for (i, chunk) in later.chunks(4).enumerate() {
            let at = t0 + Duration::from_secs(10 + i as u64);
            let (snap, notes) = hub.resolve_noting(chunk, 0, || at);
            assert_eq!(
                (snap.len, snap.flags, notes.overflow),
                (0, FLAG_OTHER, None)
            );
            let (_, notes) = hub.resolve_noting(&chunk[..1], 0, || at);
            assert_eq!(notes.overflow, None);
        }
        let at = t0 + OVERFLOW_LOG_INTERVAL;
        let (_, notes) = hub.resolve_noting(&[key("late1")], 0, || at);
        let line = notes.overflow.unwrap();
        let listed: Vec<String> = (1..=16).map(|i| format!("late{i}")).collect();
        assert!(
            // late17..late20, and late17 again: unlisted names aren't remembered.
            line.ends_with(&format!(
                "include: {}, and 5 more sightings",
                listed.join(", ")
            )),
            "{line}"
        );
        // The memory is bounded and cleared after each line.
        let (_, notes) = hub.resolve_noting(&[key("late2")], 0, || at + Duration::from_secs(59));
        assert_eq!(notes.overflow, None);
        // No slot was ever taken past the cap.
        assert_eq!(hub.current().2.len(), MAX_REGISTRY);
    }

    #[test]
    fn identity_states_have_their_own_columns() {
        let (hub, _gate) = live();
        let snap = snap_for(&hub, "SELECT * FROM a, generate_series(1, 2)");
        for state in IdentityState::ALL {
            hub.admit_noting(snap, state, || ());
        }
        hub.admit_noting(snap, IdentityState::Expired, || ());
        assert_eq!(count(&hub, "a"), [1, 1, 2, 1]);
        assert_eq!(count(&hub, "unknown"), [1, 1, 2, 1]);
    }

    #[test]
    fn callers_are_logged_once_per_relation_and_state_without_identity() {
        let (hub, _gate) = live();
        let ab = snap_for(&hub, "SELECT * FROM a, b");
        let b = snap_for(&hub, "SELECT * FROM b, generate_series(1, 2)");
        let names = |v: Vec<Box<str>>| -> Vec<String> { v.into_iter().map(String::from).collect() };
        assert!(hub.admit_noting(ab, PRESENT, || ()).is_empty());
        assert_eq!(names(hub.admit_noting(ab, ABSENT, || ())), ["a", "b"]);
        assert!(hub.admit_noting(ab, ABSENT, || ()).is_empty());
        assert!(hub.admit_noting(b, ABSENT, || ()).is_empty());
        assert_eq!(
            names(hub.admit_noting(b, IdentityState::Expired, || ())),
            ["b"]
        );
        assert_eq!(
            names(hub.admit_noting(ab, IdentityState::Expired, || ())),
            ["a"]
        );
        assert_eq!(
            names(hub.admit_noting(ab, IdentityState::Refused, || ())),
            ["a", "b"]
        );
        assert!(hub
            .admit_noting(ab, IdentityState::Refused, || ())
            .is_empty());
        // The logging wrapper counts like the rest.
        hub.admit(
            ab,
            Caller {
                identity: IdentityState::Refused,
                application_name: "app\n",
                address: Some("127.0.0.1:5432".parse().unwrap()),
            },
        );
        assert_eq!(count(&hub, "a"), [1, 2, 1, 3]);
    }

    #[test]
    fn client_text_is_escaped_in_log_lines() {
        assert!(matches!(log_escape("orders"), Cow::Borrowed("orders")));
        assert_eq!(
            log_escape("a\nb\r\t\u{1b}[2J\\\"\u{202e}\u{61c}\u{85}\u{2028}é"),
            r#"a\nb\r\t\u{1b}[2J\\\"\u{202e}\u{61c}\u{85}\u{2028}é"#
        );
        assert_ne!(log_escape("a\\nb"), log_escape("a\nb"));
        // A quoted name with a line break and a terminal escape, through the
        // classifier and the callers log, with a hostile application name.
        let (hub, _gate) = live();
        let snap = snap_for(
            &hub,
            "SELECT * FROM \"x\n2026-11-09T00:00:00Z INFO forged \u{1b}[2J\"",
        );
        let sighted = hub.admit_noting(snap, ABSENT, || ());
        assert_eq!(sighted.len(), 1);
        let caller = Caller {
            identity: ABSENT,
            application_name: "app\r\n\u{1b}]0;title\u{7}\"",
            address: Some("127.0.0.1:5432".parse().unwrap()),
        };
        let line = hub.callers_line(&sighted[0], &caller);
        assert!(!line.chars().any(char::is_control), "{line:?}");
        assert_eq!(
            line,
            r#"[pool: db][user: app] relation "\"x\n2026-11-09T00:00:00Z INFO forged \u{1b}[2J\"" first counted with identity absent: application_name "app\r\n\u{1b}]0;title\u{7}\"", client 127.0.0.1:5432"#
        );
    }

    // --- Epochs, publication, admission ---

    #[test]
    fn another_lineage_or_publication_is_unknown() {
        let (hub, mut gate) = live();
        let other = RelationHub::default();
        gate.install(&other);
        // Slot 0 of each hub is a different name.
        let stale = snap_for(&hub, "SELECT * FROM a");
        snap_for(&other, "SELECT * FROM b");
        assert_ne!(hub.current().0, other.current().0);
        other.admit_noting(stale, PRESENT, || ());
        assert_eq!(count(&other, "b"), [0, 0, 0, 0]);
        assert_eq!(count(&other, "unknown"), [1, 0, 0, 0]);
        // Retired and published again: a fresh, empty registry and epoch.
        let epoch = hub.current().0;
        gate.publish(&[&hub, &other], &[&other]);
        assert_eq!(hub.current(), (0, false, vec![]));
        assert!(hub.samples().is_empty());
        hub.admit_noting(stale, ABSENT, || ());
        gate.publish(&[&other], &[&hub, &other]);
        assert!(hub.current().0 > epoch);
        snap_for(&hub, "SELECT * FROM z");
        hub.admit_noting(stale, ABSENT, || ());
        assert_eq!(count(&hub, "z"), [0, 0, 0, 0]);
        assert_eq!(count(&hub, "unknown"), [0, 1, 0, 0]);
    }

    #[test]
    fn exhausted_epochs_count_unknown_only() {
        let mut gate = Gate::seeded(u32::MAX - 1);
        let hub = RelationHub::default();
        gate.install(&hub);
        let last = hub.current().0;
        assert_eq!(last, u32::MAX);
        let snap = snap_for(&hub, "SELECT * FROM a");
        hub.admit_noting(snap, PRESENT, || ());
        assert_eq!(count(&hub, "a"), [1, 0, 0, 0]);
        // Out of epochs: a republished hub is epoch 0 and counts unknown only.
        gate.publish(&[&hub], &[]);
        gate.install(&hub);
        assert_eq!(hub.current(), (0, true, vec![]));
        let fresh = snap_for(&hub, "SELECT * FROM a");
        assert_eq!(fresh, RelationSnap::UNKNOWN);
        hub.admit_noting(fresh, PRESENT, || ());
        hub.admit_noting(snap, PRESENT, || ());
        assert_eq!(hub.samples().len(), 3);
        assert_eq!(count(&hub, "unknown"), [2, 0, 0, 0]);
    }

    #[test]
    fn opposite_order_publication_gets_epochs_at_commit() {
        let hub1 = RelationHub::default();
        let hub2 = RelationHub::default();
        let mut gate = Gate::new();
        // Prepared 1 then 2, committed 2 then 1.
        gate.publish(&[], &[&hub2]);
        gate.publish(&[&hub2], &[&hub1]);
        assert_eq!(hub1.current().0, 2);
        assert_eq!(hub2.current(), (0, false, vec![]));
    }

    #[test]
    fn lock_race_old_attempt_completes_before_retirement() {
        let (hub, mut gate) = live();
        let hub = Arc::new(hub);
        let snap = snap_for(&hub, "SELECT * FROM a");
        let (in_guard_tx, in_guard_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let attempt = {
            let hub = hub.clone();
            std::thread::spawn(move || {
                hub.admit_noting(snap, PRESENT, || {
                    in_guard_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        in_guard_rx.recv().unwrap();
        // Neither a retirement nor a new name can start while the attempt
        // holds the read guard.
        assert!(hub.raw_bank().try_write().is_none());
        release_tx.send(()).unwrap();
        attempt.join().unwrap();
        assert_eq!(count(&hub, "a"), [1, 0, 0, 0]);
        gate.publish(&[&hub], &[]);
        gate.install(&hub);
        assert_eq!(hub.current().2, Vec::<String>::new());
        assert_eq!(count(&hub, "unknown"), [0, 0, 0, 0]);
    }

    #[test]
    fn republication_between_classify_and_admit_is_unknown() {
        let (hub, mut gate) = live();
        let snap = snap_for(&hub, "SELECT * FROM a");
        gate.publish(&[&hub], &[]);
        gate.install(&hub);
        // The new registry's slot 0 is another name.
        snap_for(&hub, "SELECT * FROM b");
        hub.admit_noting(snap, ABSENT, || ());
        assert_eq!(count(&hub, "b"), [0, 0, 0, 0]);
        assert_eq!(count(&hub, "unknown"), [0, 1, 0, 0]);
    }

    #[test]
    fn lock_race_one_new_name_from_many_threads_takes_one_slot() {
        let (hub, _gate) = live();
        let hub = Arc::new(hub);
        let threads = 8;
        let start = Arc::new(std::sync::Barrier::new(threads));
        let racers: Vec<_> = (0..threads)
            .map(|_| {
                let (hub, start) = (hub.clone(), start.clone());
                std::thread::spawn(move || {
                    start.wait();
                    let snap = snap_for(&hub, "SELECT * FROM same");
                    hub.admit_noting(snap, PRESENT, || ());
                    snap.slots().to_vec()
                })
            })
            .collect();
        for racer in racers {
            assert_eq!(racer.join().unwrap(), [0]);
        }
        assert_eq!(hub.current().2, ["same"]);
        assert_eq!(count(&hub, "same"), [threads as u64, 0, 0, 0]);
    }

    #[test]
    fn snapshot_without_epoch_counts_unknown() {
        let (hub, _gate) = live();
        hub.admit_noting(RelationSnap::UNKNOWN, PRESENT, || ());
        assert_eq!(count(&hub, "unknown"), [1, 0, 0, 0]);
    }

    #[test]
    fn unpublished_hub_classifies_unknown_and_counts_nothing() {
        let hub = RelationHub::default();
        assert_eq!(snap_for(&hub, "SELECT * FROM a"), RelationSnap::UNKNOWN);
        hub.admit_noting(RelationSnap::UNKNOWN, PRESENT, || ());
        assert!(hub.samples().is_empty());
        assert!(hub.current().2.is_empty());
    }

    #[test]
    fn snapshot_and_scratch_sizes() {
        assert_eq!(std::mem::size_of::<RelationSnap>(), 40);
        let total = MAX_KEYS * std::mem::size_of::<KeyBuf>()
            + MAX_ALIASES * std::mem::size_of::<AliasSlot>()
            + MAX_SCOPES * std::mem::size_of::<u16>()
            + std::mem::size_of::<Classifier>();
        assert!(total <= SCRATCH_BUDGET, "{total}");
        // A hub that never counts a name is this, and nothing on the heap
        // (see `storage_is_allocated_as_names_arrive`); a chunk is 64 rows.
        let hub = std::mem::size_of::<RelationHub>();
        assert!(hub <= 512, "{hub}");
        let bank = std::mem::size_of::<Bank>();
        assert!(bank < hub, "{bank}");
        assert_eq!(std::mem::size_of::<Chunk>(), CHUNK_SLOTS * 40);
    }
}
