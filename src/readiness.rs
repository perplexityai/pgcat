//! Readiness for load balancers: not ready until every pool has opened its
//! `min_pool_size` connections, ready from then on, and not ready again once
//! a graceful shutdown starts draining clients.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::pool::{get_all_pools, pools_generation};

/// Latched, so a pool dipping below its minimum while bb8 replaces
/// connections at `server_lifetime` doesn't flap the probe.
static WARMED: AtomicBool = AtomicBool::new(false);
static DRAINING: AtomicBool = AtomicBool::new(false);

pub fn start_draining() {
    DRAINING.store(true, Ordering::Relaxed);
}

pub fn is_ready() -> bool {
    let warm = WARMED.load(Ordering::Relaxed) || warmed();
    if warm {
        WARMED.store(true, Ordering::Relaxed);
    }
    warm && !DRAINING.load(Ordering::Relaxed)
}

/// Whether the pools are published and each holds its minimum connections.
fn warmed() -> bool {
    pools_generation() > 0
        && get_all_pools()
            .values()
            .all(|pool| pool.has_min_connections())
}
