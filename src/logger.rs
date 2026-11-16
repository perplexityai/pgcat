use crate::cmd_args::{Args, LogFormat};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

pub fn init(args: &Args) {
    // Initialize a default filter, and then override the builtin default "warning" with our
    // commandline, (default: "info")
    let filter = EnvFilter::from_default_env().add_directive(args.log_level.into());

    // sqlparser logs complete SQL and tokens. This must remain suppressed even
    // when RUST_LOG explicitly enables one of its modules.
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(filter_fn(|metadata| {
            let target = metadata.target();
            target != "sqlparser" && !target.starts_with("sqlparser::")
        }));
    let trace_sub = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_ansi(!args.no_color);

    match args.log_format {
        LogFormat::Structured => subscriber.with(trace_sub.json()).init(),
        LogFormat::Debug => subscriber.with(trace_sub.pretty()).init(),
        _ => subscriber.with(trace_sub).init(),
    };
}
