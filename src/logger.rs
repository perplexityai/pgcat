use crate::cmd_args::{Args, LogFormat};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Crates whose events are dropped at every level, even when RUST_LOG names one
/// of their modules: sqlparser logs complete SQL and tokens, and the AWS SDK's
/// orchestrator (`aws_smithy_runtime`) logs whole requests and responses, which
/// can hold keys and other secrets.
const SUPPRESSED: [&str; 2] = ["sqlparser", "aws_smithy_runtime"];

/// Whether events from `target` are dropped. Public so tests that check logs
/// for secrets filter as pgcat does.
pub fn is_suppressed(target: &str) -> bool {
    SUPPRESSED.iter().any(|name| {
        target
            .strip_prefix(name)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

pub fn init(args: &Args) {
    // Initialize a default filter, and then override the builtin default "warning" with our
    // commandline, (default: "info")
    let filter = EnvFilter::from_default_env().add_directive(args.log_level.into());

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(filter_fn(|metadata| !is_suppressed(metadata.target())));
    let trace_sub = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_ansi(!args.no_color);

    match args.log_format {
        LogFormat::Structured => subscriber.with(trace_sub.json()).init(),
        LogFormat::Debug => subscriber.with(trace_sub.pretty()).init(),
        _ => subscriber.with(trace_sub).init(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_whole_crates_only() {
        for target in [
            "sqlparser",
            "sqlparser::parser",
            "aws_smithy_runtime",
            "aws_smithy_runtime::client::orchestrator",
        ] {
            assert!(is_suppressed(target), "{}", target);
        }
        for target in [
            "sqlparser_extra",
            "aws_smithy_runtime_api::client",
            "aws_sdk_s3",
            "pgcat::client",
        ] {
            assert!(!is_suppressed(target), "{}", target);
        }
    }
}
