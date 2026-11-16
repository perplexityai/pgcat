//! A client's prepared statements as pgcat keeps them: each one's entry in the
//! pool's statement cache, its plan, what executing it does, and the client's
//! own Parse with the plan generation it was planned for, so it can be planned
//! again after a reload changes what the plugins check. A buffered batch's
//! Parses and Closes change them only if the batch is sent: a name must never
//! pair one statement's SQL with another's plan or effects.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;

use super::StatementPlan;
use crate::messages::Parse;
use crate::sql_observer::Snap;

/// The client's Parse of a statement, and the plan generation it was planned with.
pub type Origin = (BytesMut, u64);

/// A statement's entry in the pool's cache, and what the SQL observer counts
/// it as. Kept together, so a dropped batch puts both back.
#[derive(Clone, Debug)]
pub struct Cached {
    /// The statement as the pool's cache renamed it.
    pub parse: Arc<Parse>,
    pub hash: u64,
    /// A plugin changed the client's SQL text on its way to the cache.
    pub rewritten: bool,
    /// The SQL observer's classification of the client's own SQL, where the
    /// cached statement's shared one isn't it (see
    /// `ClientObserver::stage_cached_parse`). None if it is, or if the
    /// observer was off when the statement was parsed. Boxed, so the map's
    /// buckets grow by 16 B, not 48, and the 44 B `Snap` is allocated only
    /// for a statement the observer labels.
    pub label: Option<Box<Snap>>,
}

impl Cached {
    /// The entry of a statement planned again after a reload (`old` is the
    /// one it replaces), now cached as `parse`: `rewritten` if a plugin now
    /// changes the client's SQL text. The client's SQL is the same, so the
    /// observer's label stands; rewritten before or now, the statement is
    /// never counted as the cached SQL.
    pub fn replanned(
        old: Option<&Cached>,
        parse: Arc<Parse>,
        hash: u64,
        rewritten: bool,
    ) -> Cached {
        let (was_rewritten, label) = match old {
            Some(old) => (old.rewritten, old.label.clone()),
            None => (false, None),
        };
        Cached {
            parse,
            hash,
            rewritten: was_rewritten || rewritten,
            label,
        }
    }
}

/// What executing a statement does to what pgcat tracks. Executing, not
/// parsing: a prepared statement can run later, and more than once.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Effects {
    /// It resets the session (`DISCARD ALL`, `RESET ALL`), and the identity with it.
    pub resets_session: bool,

    /// It names the tenant parameter (see `tenant_may_change`).
    pub may_change_tenant: bool,

    /// It begins a transaction (see `batch_begins`).
    pub begins_transaction: bool,
}

/// What a buffered batch changed, with the value it replaced.
enum Change {
    Plan(Option<Arc<StatementPlan>>, Option<Origin>, Option<Effects>),
    Cached(Option<Cached>),
    Closed(
        Option<Arc<StatementPlan>>,
        Option<Origin>,
        Option<Effects>,
        Option<Cached>,
    ),
}

#[derive(Default)]
pub struct StatementPlans {
    /// By client statement name, the unnamed statement included.
    plans: HashMap<String, Arc<StatementPlan>>,
    origins: HashMap<String, Origin>,
    /// Only statements with an effect are kept.
    effects: HashMap<String, Effects>,
    cached: HashMap<String, Cached>,
    /// What the buffered batch changed, oldest first.
    journal: Vec<(String, Change)>,
}

impl StatementPlans {
    /// No statement has a plan.
    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    pub fn plan(&self, name: &str) -> Option<Arc<StatementPlan>> {
        self.plans.get(name).cloned()
    }

    /// Some statement was planned with a planning plugin on.
    pub fn has_origins(&self) -> bool {
        !self.origins.is_empty()
    }

    pub fn origin(&self, name: &str) -> Option<&Origin> {
        self.origins.get(name)
    }

    pub fn cached(&self, name: &str) -> Option<&Cached> {
        self.cached.get(name)
    }

    /// Some statement has an effect.
    pub fn has_effects(&self) -> bool {
        !self.effects.is_empty()
    }

    pub fn effects(&self, name: &str) -> Option<Effects> {
        self.effects.get(name).copied()
    }

    /// Change a statement's plan, origin and effects (None and no effects
    /// remove them), until the buffered batch is sent or dropped.
    pub fn set(
        &mut self,
        name: String,
        plan: Option<Arc<StatementPlan>>,
        origin: Option<Origin>,
        effects: Effects,
    ) {
        let effects = (effects != Effects::default()).then_some(effects);
        let old_plan = put(&mut self.plans, &name, plan);
        let old_origin = put(&mut self.origins, &name, origin);
        let old_effects = put(&mut self.effects, &name, effects);
        self.journal
            .push((name, Change::Plan(old_plan, old_origin, old_effects)));
    }

    /// Change a statement's cache entry (None removes it), until the buffered
    /// batch is sent or dropped.
    pub fn cache(&mut self, name: String, cached: Option<Cached>) {
        let old = put(&mut self.cached, &name, cached);
        self.journal.push((name, Change::Cached(old)));
    }

    /// Close a statement: drop its plan, origin, effects and cache entry,
    /// until the buffered batch is sent or dropped. Dropped when the Close is
    /// buffered, not when it's sent, so a Parse of the name later in the batch
    /// stands.
    pub fn close(&mut self, name: String) {
        let plan = self.plans.remove(&name);
        let origin = self.origins.remove(&name);
        let effects = self.effects.remove(&name);
        let cached = self.cached.remove(&name);
        self.journal
            .push((name, Change::Closed(plan, origin, effects, cached)));
    }

    /// Remove a statement's cache entry for good, once its batch is sent.
    pub fn uncache(&mut self, name: &str) -> Option<Cached> {
        self.cached.remove(name)
    }

    /// The buffered batch went to the server: its changes stand.
    pub fn commit(&mut self) {
        self.journal.clear();
    }

    /// The buffered batch was dropped: its Parses and Closes never happened.
    pub fn rollback(&mut self) {
        while let Some((name, change)) = self.journal.pop() {
            match change {
                Change::Plan(plan, origin, effects) => {
                    put(&mut self.plans, &name, plan);
                    put(&mut self.origins, &name, origin);
                    put(&mut self.effects, &name, effects);
                }
                Change::Cached(cached) => {
                    put(&mut self.cached, &name, cached);
                }
                Change::Closed(plan, origin, effects, cached) => {
                    put(&mut self.plans, &name, plan);
                    put(&mut self.origins, &name, origin);
                    put(&mut self.effects, &name, effects);
                    put(&mut self.cached, &name, cached);
                }
            }
        }
    }
}

/// Insert or, for None, remove; returns the value replaced.
fn put<V>(map: &mut HashMap<String, V>, name: &str, value: Option<V>) -> Option<V> {
    match value {
        Some(value) => map.insert(name.to_string(), value),
        None => map.remove(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEGINS: Effects = Effects {
        resets_session: false,
        may_change_tenant: false,
        begins_transaction: true,
    };

    fn query(statements: &StatementPlans, name: &str) -> Option<String> {
        statements
            .cached(name)
            .map(|cached| cached.parse.query().to_string())
    }

    #[test]
    fn a_close_then_a_parse_of_the_name_leaves_the_parse() {
        let mut statements = StatementPlans::default();
        statements.cache("s".into(), Some(cached("SELECT 'A'", false, None)));
        statements.set("s".into(), None, None, BEGINS);
        statements.commit();

        // [Close s, Parse s=B], dropped: s is A again, with its effects.
        statements.close("s".into());
        assert_eq!(query(&statements, "s"), None);
        statements.cache("s".into(), Some(cached("SELECT 'B'", false, None)));
        statements.set("s".into(), None, None, Effects::default());
        statements.rollback();
        assert_eq!(query(&statements, "s").as_deref(), Some("SELECT 'A'"));
        assert_eq!(statements.effects("s"), Some(BEGINS));

        // Sent: s is B.
        statements.close("s".into());
        statements.cache("s".into(), Some(cached("SELECT 'B'", false, None)));
        statements.set("s".into(), None, None, Effects::default());
        statements.commit();
        assert_eq!(query(&statements, "s").as_deref(), Some("SELECT 'B'"));

        // A Close alone, sent, drops it; dropped, keeps it.
        statements.close("s".into());
        statements.rollback();
        assert_eq!(query(&statements, "s").as_deref(), Some("SELECT 'B'"));
        statements.close("s".into());
        statements.commit();
        assert_eq!(query(&statements, "s"), None);
    }

    #[test]
    fn a_parse_then_a_close_of_the_name_dropped_puts_back_what_was_there() {
        let mut statements = StatementPlans::default();
        statements.cache("s".into(), Some(cached("SELECT 'A'", false, None)));
        statements.set("s".into(), None, None, BEGINS);
        statements.commit();

        // [Parse s=B, Close s], dropped: s is A again, with its effects.
        statements.cache("s".into(), Some(cached("SELECT 'B'", false, None)));
        statements.set("s".into(), None, None, Effects::default());
        statements.close("s".into());
        assert_eq!(query(&statements, "s"), None);
        statements.rollback();
        assert_eq!(query(&statements, "s").as_deref(), Some("SELECT 'A'"));
        assert_eq!(statements.effects("s"), Some(BEGINS));
    }

    #[test]
    fn a_dropped_batch_puts_a_statements_effects_back() {
        let mut statements = StatementPlans::default();
        statements.set("b".into(), None, None, BEGINS);
        statements.commit();

        // Parsed again, as a statement without effects, in a batch that's dropped.
        statements.set("b".into(), None, None, Effects::default());
        assert_eq!(statements.effects("b"), None);
        statements.rollback();
        assert_eq!(statements.effects("b"), Some(BEGINS));

        // Closed in a batch that's sent.
        statements.set("b".into(), None, None, Effects::default());
        statements.commit();
        statements.rollback();
        assert!(!statements.has_effects());
    }

    fn cached(sql: &str, rewritten: bool, label: Option<Snap>) -> Cached {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_u8(b'P');
        buf.put_i32((4 + 1 + sql.len() + 1 + 2) as i32);
        buf.put_u8(0);
        buf.put_slice(sql.as_bytes());
        buf.put_u8(0);
        buf.put_i16(0);
        let parse = Parse::try_from(&buf).expect("parse");
        Cached {
            parse: Arc::new(parse),
            hash: sql.len() as u64,
            rewritten,
            label: label.map(Box::new),
        }
    }

    // The SQL observer's label and the rewritten flag are rolled
    // back with the cache entry they describe.
    #[test]
    fn a_dropped_batch_puts_a_statements_label_back() {
        let mut statements = StatementPlans::default();
        statements.cache(
            "".into(),
            Some(cached("SELECT 'A'", false, Some(Snap::Excluded))),
        );
        statements.cache("s".into(), Some(cached("SELECT 'A'", false, None)));
        statements.commit();

        statements.cache(
            "".into(),
            Some(cached("SELECT 'B'", true, Some(Snap::Unparsed))),
        );
        statements.cache(
            "s".into(),
            Some(cached("SELECT 'B'", true, Some(Snap::Unparsed))),
        );
        statements.cache("t".into(), Some(cached("SELECT 'B'", false, None)));
        statements.rollback();
        for name in ["", "s"] {
            let entry = statements.cached(name).expect("held");
            assert_eq!(entry.parse.query(), "SELECT 'A'");
            assert!(!entry.rewritten);
        }
        let label = |name| {
            statements
                .cached(name)
                .and_then(|c| c.label.as_deref().copied())
        };
        assert_eq!(label(""), Some(Snap::Excluded));
        assert_eq!(label("s"), None);
        assert!(statements.cached("t").is_none());

        // Sent, the changes stand.
        statements.cache(
            "s".into(),
            Some(cached("SELECT 'B'", true, Some(Snap::Unparsed))),
        );
        statements.commit();
        statements.rollback();
        let entry = statements.cached("s").expect("held");
        assert!(entry.rewritten);
        assert_eq!(entry.label.as_deref(), Some(&Snap::Unparsed));
    }
}
