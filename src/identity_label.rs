//! The `identity` label of the SQL observer's metrics: whether the client had a
//! verified `PGCAT_IDENTITY` when a statement was counted, and if not, why.
//!
//! Labels only. The client's identity (`Client::identity`) alone decides
//! authorization and what plugins see; this records why it is `None`, and
//! nothing reads it but the observer.

/// One `identity` label value. The discriminant indexes counter columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IdentityState {
    /// A verified, unexpired `PGCAT_IDENTITY`.
    Present = 0,
    /// Never set, or cleared by `RESET PGCAT_IDENTITY`, `DISCARD ALL` or `RESET ALL`.
    Absent = 1,
    /// The identity's token expired.
    Expired = 2,
    /// The last `SET PGCAT_IDENTITY` was refused.
    Refused = 3,
}

impl IdentityState {
    pub const COUNT: usize = 4;
    /// In column order.
    pub const ALL: [IdentityState; IdentityState::COUNT] = [
        IdentityState::Present,
        IdentityState::Absent,
        IdentityState::Expired,
        IdentityState::Refused,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            IdentityState::Present => "present",
            IdentityState::Absent => "absent",
            IdentityState::Expired => "expired",
            IdentityState::Refused => "refused",
        }
    }

    pub fn column(self) -> usize {
        self as usize
    }
}

/// Why a client without an identity has none: `expired` and `refused` stick
/// until the next successful `SET PGCAT_IDENTITY` or a reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdentityLapse(IdentityState);

impl Default for IdentityLapse {
    fn default() -> IdentityLapse {
        IdentityLapse(IdentityState::Absent)
    }
}

impl IdentityLapse {
    /// A successful `SET PGCAT_IDENTITY`, `RESET PGCAT_IDENTITY`, `DISCARD ALL`
    /// or `RESET ALL`.
    pub fn clear(&mut self) {
        self.0 = IdentityState::Absent;
    }

    /// The client's identity expired and was forgotten.
    pub fn expired(&mut self) {
        self.0 = IdentityState::Expired;
    }

    /// A `SET PGCAT_IDENTITY` was refused (and any identity forgotten).
    pub fn refused(&mut self) {
        self.0 = IdentityState::Refused;
    }

    /// The label, given whether the client has an identity now.
    pub fn state(&self, present: bool) -> IdentityState {
        if present {
            IdentityState::Present
        } else {
            self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_follow_all() {
        for (i, state) in IdentityState::ALL.iter().enumerate() {
            assert_eq!(state.column(), i);
        }
        let labels: Vec<_> = IdentityState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(labels, ["present", "absent", "expired", "refused"]);
    }

    #[test]
    fn transitions() {
        let mut lapse = IdentityLapse::default();
        // Never set.
        assert_eq!(lapse.state(false), IdentityState::Absent);
        // Set: present, whatever came before.
        lapse.clear();
        assert_eq!(lapse.state(true), IdentityState::Present);
        // Expired: sticks while there is no identity.
        lapse.expired();
        assert_eq!(lapse.state(false), IdentityState::Expired);
        assert_eq!(lapse.state(false), IdentityState::Expired);
        // A new successful SET: present; then RESET: absent.
        lapse.clear();
        assert_eq!(lapse.state(true), IdentityState::Present);
        lapse.clear();
        assert_eq!(lapse.state(false), IdentityState::Absent);
        // Refused (from absent, or replacing an expired one): sticks.
        lapse.expired();
        lapse.refused();
        assert_eq!(lapse.state(false), IdentityState::Refused);
        // A successful SET after a refusal: present.
        lapse.clear();
        assert_eq!(lapse.state(true), IdentityState::Present);
        // Refused while present: the identity is dropped by the caller.
        lapse.refused();
        assert_eq!(lapse.state(false), IdentityState::Refused);
        // DISCARD ALL / RESET ALL / RESET PGCAT_IDENTITY: absent.
        lapse.clear();
        assert_eq!(lapse.state(false), IdentityState::Absent);
    }
}
