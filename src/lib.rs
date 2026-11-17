// Preserve upstream's pgcat:: paths when compiling its entry point as a module;
// rewriting them to crate:: would add unrelated churn to future upstream syncs.
extern crate self as pgcat;

// Keep the startup loop at upstream's src/main.rs path so incoming fixes merge
// into the code we execute. Moving it elsewhere while leaving a wrapper at that
// path defeats Git's rename detection and requires manual transplants on sync.
// The standalone binary has its own root and allocator in src/bin/pgcat.rs;
// this library never selects an allocator for the executable importing it.
#[path = "main.rs"]
mod upstream_main;

pub mod admin;
pub mod auth_passthrough;
pub mod client;
pub mod cmd_args;
pub mod codec;
pub mod config;
pub mod constants;
pub mod dns_cache;
pub mod errors;
#[cfg(test)]
pub mod fuzz;
pub mod held_replies;
pub mod identity;
pub mod identity_label;
pub mod logger;
pub mod messages;
pub mod mirrors;
pub mod plugins;
pub mod pool;
pub mod prometheus;
pub mod query_router;
pub mod readiness;
pub mod relations;
pub mod runtime;
pub mod scram;
pub mod server;
pub mod sharding;
pub mod sql_observer;
pub mod stats;
#[cfg(test)]
mod test_allocator;
pub mod tls;

/// Format chrono::Duration to be more human-friendly.
///
/// # Arguments
///
/// * `duration` - A duration of time
pub fn format_duration(duration: &chrono::Duration) -> String {
    let milliseconds = format!("{:0>3}", duration.num_milliseconds() % 1000);

    let seconds = format!("{:0>2}", duration.num_seconds() % 60);

    let minutes = format!("{:0>2}", duration.num_minutes() % 60);

    let hours = format!("{:0>2}", duration.num_hours() % 24);

    let days = duration.num_days().to_string();

    format!(
        "{}d {}:{}:{}.{}",
        days, hours, minutes, seconds, milliseconds
    )
}
