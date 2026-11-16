//! Keep the embedding adapter separate so plugin registration does not grow
//! fork-only startup logic in upstream's `main.rs`.

use crate::plugins::registry::PluginRegistry;

/// Run the standalone server without externally registered plugins.
///
/// This has the same process-owning contract as [`run_with_plugins`].
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    run_with_plugins(PluginRegistry::default())
}

/// Own PgCat's process-level startup. Call once on the main thread, outside a
/// Tokio runtime; plugins are installed before config loading or client admission.
///
/// This retains the standalone CLI, global logger, signal handlers and runtimes.
/// Upstream's query-router setup, config parsing, metrics-address parsing,
/// listener binding and pool creation failures exit with `exitcode::CONFIG`;
/// they do not return `Err`.
/// Other fallible setup and duplicate registry installation can return `Err`.
/// This is not an in-process, restartable server API.
pub fn run_with_plugins(plugins: PluginRegistry) -> Result<(), Box<dyn std::error::Error>> {
    crate::plugins::registry::install(plugins)
        .map_err(|_| "plugin registry was already installed")?;
    crate::upstream_main::run_server()
}
