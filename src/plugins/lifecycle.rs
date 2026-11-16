//! Starting and stopping the configured plugins with pgcat, and their lines
//! on the Prometheus `/metrics` page.
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use log::{error, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::registry::{process_plugins, PluginRegistry, Section};
use crate::config::Config;
use crate::errors::Error;

/// How long pgcat waits for each plugin's `start`: above the bounds plugins
/// put on their own startup I/O (30 s for a policy fetch, say), so it only
/// catches one that hangs.
pub const START_TIMEOUT: Duration = Duration::from_secs(60);

/// How long pgcat waits for each plugin's `shutdown`.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The configured plugins that started, for [`Started::shutdown`] to stop.
#[must_use = "shut the started plugins down"]
pub struct Started {
    plugins: Arc<PluginRegistry>,
    /// The first `count` configured plugins, in registration order, started.
    count: usize,
}

/// Start the process's configured plugins, in registration order, each told
/// whether a pool in `config` turns it on. If one fails, the plugins started
/// before it are shut down and pgcat must not run.
pub async fn start(config: &Config) -> Result<Started, Error> {
    start_plugins(process_plugins(), config).await
}

pub(crate) async fn start_plugins(
    plugins: Arc<PluginRegistry>,
    config: &Config,
) -> Result<Started, Error> {
    let mut started = Started {
        plugins: Arc::clone(&plugins),
        count: 0,
    };
    for (name, plugin) in plugins.configured_plugins() {
        let start = plugin.start(enabled(&plugins, config, name));
        let ok = match tokio::time::timeout(START_TIMEOUT, start).await {
            Ok(Ok(())) => true,
            // The plugin logs why: its error can hold what pgcat mustn't log.
            Ok(Err(_)) => {
                error!("Plugin {:?} failed to start", name);
                false
            }
            // Dropped, not shut down: it didn't start.
            Err(_) => {
                error!("Plugin {:?} didn't start within {:?}", name, START_TIMEOUT);
                false
            }
        };
        if !ok {
            started.shutdown().await;
            return Err(Error::BadConfig);
        }
        started.count += 1;
    }
    Ok(started)
}

/// Whether a pool turns the plugin on, in its own `[plugins]` or, without
/// them, in the global ones, as the pool's settings resolve them.
fn enabled(registry: &PluginRegistry, config: &Config, name: &str) -> bool {
    config.pools.values().any(|pool| {
        pool.plugins_inheriting(config.plugins.as_ref())
            .and_then(|plugins| plugins.configured.get(name))
            .map_or(false, |section| {
                matches!(
                    Section::resolve(registry.configured_plugin(name), section),
                    Section::On(..)
                )
            })
    })
}

impl Started {
    /// Shut the started plugins down, the last started first. pgcat waits
    /// [`SHUTDOWN_TIMEOUT`] for each at most, so one that hangs can't keep
    /// pgcat from exiting or the others from shutting down.
    pub async fn shutdown(self) {
        let started: Vec<_> = self.plugins.configured_plugins().take(self.count).collect();
        for (name, plugin) in started.into_iter().rev() {
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, plugin.shutdown())
                .await
                .is_err()
            {
                warn!(
                    "Plugin {:?} didn't shut down within {:?}",
                    name, SHUTDOWN_TIMEOUT
                );
            }
        }
    }
}

/// The configured plugins' lines for the Prometheus `/metrics` page, whether
/// or not they're on or started.
pub(crate) fn prometheus_lines(lines: &mut Vec<String>) {
    registry_lines(&process_plugins(), lines);
}

/// The plugins that added a line pgcat dropped, warned about once each.
static DROPPED_LINES: Lazy<Mutex<HashSet<&'static str>>> = Lazy::new(Default::default);

/// Each plugin's lines that are one text-format line of its own metrics, so
/// one it gets wrong can't fail the scrape of the others, or add to theirs:
/// `register_configured` keeps each plugin's `pgcat_<name>_` prefix its own.
fn registry_lines(registry: &PluginRegistry, lines: &mut Vec<String>) {
    let mut added = Vec::new();
    for (name, plugin) in registry.configured_plugins() {
        plugin.prometheus_lines(&mut added);
        let prefix = format!("pgcat_{}_", name);
        let before = added.len();
        added.retain(|line| plugin_line(&prefix, line));
        if added.len() < before && DROPPED_LINES.lock().insert(name) {
            warn!(
                "Plugin {:?} added metric lines pgcat drops: not its own, or not valid",
                name
            );
        }
        lines.append(&mut added);
    }
}

/// Whether `line` is one Prometheus text-format line, in the canonical form,
/// of a metric named `<prefix>...`: `# HELP`, `# TYPE` or a sample.
fn plugin_line(prefix: &str, line: &str) -> bool {
    if line.contains(['\n', '\r']) {
        return false;
    }
    if let Some(help) = line.strip_prefix("# HELP ") {
        let (name, text) = help.split_once(' ').unwrap_or((help, ""));
        return metric_name(prefix, name) && help_text(text);
    }
    if let Some(kind) = line.strip_prefix("# TYPE ") {
        return kind.split_once(' ').map_or(false, |(name, kind)| {
            metric_name(prefix, name)
                && matches!(
                    kind,
                    "counter" | "gauge" | "histogram" | "summary" | "untyped"
                )
        });
    }
    let end = line.find(['{', ' ']).unwrap_or(line.len());
    let (name, rest) = line.split_at(end);
    let rest = match rest.strip_prefix('{') {
        Some(labels) => match after_labels(labels) {
            Some(rest) => rest,
            None => return false,
        },
        None => rest,
    };
    // ` <value>`, then ` <timestamp>` or nothing.
    let mut fields = match rest.strip_prefix(' ') {
        Some(fields) => fields.split(' '),
        None => return false,
    };
    metric_name(prefix, name)
        && fields.next().map_or(false, sample_value)
        && fields.next().map_or(true, timestamp)
        && fields.next().is_none()
}

/// A sample's value: a finite float, or `NaN`, `+Inf` or `-Inf`. Not one Rust
/// parses as infinite (`1e9999`), which Go's parser refuses as out of range.
fn sample_value(value: &str) -> bool {
    matches!(value, "NaN" | "+Inf" | "-Inf") || value.parse::<f64>().map_or(false, f64::is_finite)
}

/// A sample's timestamp, in milliseconds: digits only, which is what
/// Prometheus's own lexer takes, without a sign.
fn timestamp(at: &str) -> bool {
    at.bytes().all(|b| b.is_ascii_digit()) && at.parse::<i64>().is_ok()
}

fn metric_name(prefix: &str, name: &str) -> bool {
    name.len() > prefix.len()
        && name.starts_with(prefix)
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
}

/// A `# HELP` docstring in the canonical form: a backslash only escapes
/// another one or a line feed (`\\`, `\n`), as the text format specifies.
/// Parsers that follow it strictly (prometheus/common's `expfmt`) fail the
/// whole page on any other escape.
fn help_text(text: &str) -> bool {
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' && !matches!(chars.next(), Some('\\' | 'n')) {
            return false;
        }
    }
    true
}

/// What follows the `}` of the label set `labels` starts (after its `{`):
/// `name="value"` pairs, comma-separated, a trailing comma allowed. A name
/// appears once, and isn't `__name__`, which is reserved for the metric's.
fn after_labels(mut labels: &str) -> Option<&str> {
    let mut names = Vec::new();
    loop {
        if let Some(rest) = labels.strip_prefix('}') {
            return Some(rest);
        }
        let (name, value) = labels.split_once("=\"")?;
        let mut chars = name.bytes();
        if !chars
            .next()
            .map_or(false, |b| b.is_ascii_alphabetic() || b == b'_')
            || !chars.all(|b| b.is_ascii_alphanumeric() || b == b'_')
            || name == "__name__"
            || names.contains(&name)
        {
            return None;
        }
        names.push(name);
        let mut chars = value.char_indices();
        let end = loop {
            match chars.next()? {
                (end, '"') => break end,
                (_, '\\') => match chars.next()? {
                    (_, '\\' | '"' | 'n') => {}
                    _ => return None,
                },
                _ => {}
            }
        };
        labels = &value[end + 1..];
        if let Some(rest) = labels.strip_prefix(',') {
            labels = rest;
        } else if !labels.starts_with('}') {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde::Deserialize;
    use sqlparser::ast::Statement;

    use super::*;
    use crate::config::{Plugins, Pool};
    use crate::plugins::registry::ConfiguredPlugin;
    use crate::plugins::{PluginContext, PluginOutput, PluginResult};

    #[derive(Deserialize)]
    struct OnOff {
        enabled: bool,
    }

    /// What the plugins were asked to do, in order.
    type Events = Arc<Mutex<Vec<String>>>;

    enum Behavior {
        Starts,
        FailsToStart,
        HangsOnStart,
        HangsOnShutdown,
        AddsBadLines,
    }

    struct Recorder {
        name: &'static str,
        behavior: Behavior,
        events: Events,
    }

    impl Recorder {
        fn record(&self, event: String) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[async_trait]
    impl ConfiguredPlugin for Recorder {
        type Config = OnOff;

        fn enabled(&self, config: &OnOff) -> bool {
            config.enabled
        }

        async fn on_query(
            &self,
            _config: &OnOff,
            _context: &PluginContext<'_>,
            _statements: &[Statement],
        ) -> Result<PluginResult, Error> {
            Ok(PluginOutput::Allow.into())
        }

        async fn start(&self, enabled: bool) -> Result<(), Error> {
            self.record(format!("start {} {}", self.name, enabled));
            match self.behavior {
                Behavior::FailsToStart => Err(Error::BadConfig),
                Behavior::HangsOnStart => std::future::pending().await,
                Behavior::Starts | Behavior::HangsOnShutdown | Behavior::AddsBadLines => Ok(()),
            }
        }

        async fn shutdown(&self) {
            self.record(format!("shutdown {}", self.name));
            if let Behavior::HangsOnShutdown = self.behavior {
                std::future::pending::<()>().await;
            }
            self.record(format!("shut down {}", self.name));
        }

        fn prometheus_lines(&self, lines: &mut Vec<String>) {
            if let Behavior::AddsBadLines = self.behavior {
                lines.extend(BAD_LINES.iter().map(|line| line.replace("NAME", self.name)));
            }
            lines.push(format!("pgcat_{}_up 1", self.name));
        }
    }

    fn registry(plugins: Vec<(&'static str, Behavior)>) -> (Arc<PluginRegistry>, Events) {
        let events = Events::default();
        let mut registry = PluginRegistry::default();
        for (name, behavior) in plugins {
            registry
                .register_configured(
                    name,
                    Recorder {
                        name,
                        behavior,
                        events: events.clone(),
                    },
                )
                .unwrap();
        }
        (Arc::new(registry), events)
    }

    /// `[plugins]` as `registry` loads them.
    fn plugins(registry: &PluginRegistry, toml: &str) -> Plugins {
        let mut plugins: Plugins = toml::from_str(toml).unwrap();
        registry.load_sections(&mut plugins).unwrap();
        plugins
    }

    fn config(global: Option<Plugins>, pools: Vec<Option<Plugins>>) -> Config {
        let mut config = Config {
            plugins: global,
            ..Default::default()
        };
        for (index, plugins) in pools.into_iter().enumerate() {
            config.pools.insert(
                format!("pool{}", index),
                Pool {
                    plugins,
                    ..Default::default()
                },
            );
        }
        config
    }

    fn events(events: &Events) -> Vec<String> {
        events.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn plugins_start_in_order_and_shut_down_in_reverse() {
        let (registry, recorded) = registry(vec![
            ("first", Behavior::Starts),
            ("second", Behavior::Starts),
            ("third", Behavior::Starts),
        ]);
        // `first` is on in a pool's own section, `second` in the global one
        // a pool inherits; `third` is off everywhere.
        let own = plugins(&registry, "[first]\nenabled = true");
        let global = plugins(
            &registry,
            "[first]\nenabled = false\n[second]\nenabled = true\n[third]\nenabled = false",
        );
        let config = config(Some(global), vec![Some(own), None]);

        let started = start_plugins(registry, &config).await.unwrap();
        assert_eq!(
            events(&recorded),
            ["start first true", "start second true", "start third false"]
        );
        started.shutdown().await;
        assert_eq!(
            events(&recorded)[3..],
            [
                "shutdown third",
                "shut down third",
                "shutdown second",
                "shut down second",
                "shutdown first",
                "shut down first",
            ]
        );
    }

    #[tokio::test]
    async fn a_global_section_no_pool_inherits_does_not_enable() {
        let (registry, recorded) = registry(vec![("first", Behavior::Starts)]);
        let global = plugins(&registry, "[first]\nenabled = true");
        let config = config(Some(global), vec![Some(Plugins::default())]);
        start_plugins(registry, &config)
            .await
            .unwrap()
            .shutdown()
            .await;
        assert_eq!(events(&recorded)[0], "start first false");
    }

    #[tokio::test]
    async fn a_failed_start_shuts_down_the_plugins_started_before() {
        let (registry, recorded) = registry(vec![
            ("first", Behavior::Starts),
            ("second", Behavior::FailsToStart),
            ("third", Behavior::Starts),
        ]);
        let result = start_plugins(registry, &config(None, vec![None])).await;
        assert!(matches!(result, Err(Error::BadConfig)));
        // `third` never starts; `second` isn't shut down, as it didn't start.
        assert_eq!(
            events(&recorded),
            [
                "start first false",
                "start second false",
                "shutdown first",
                "shut down first",
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_plugin_that_hangs_on_start_fails_after_the_timeout() {
        let (registry, recorded) = registry(vec![
            ("first", Behavior::Starts),
            ("second", Behavior::HangsOnStart),
            ("third", Behavior::Starts),
        ]);
        let before = tokio::time::Instant::now();
        // The outer bound fails the test, rather than hanging it, if `start`
        // ran without one.
        let result = tokio::time::timeout(
            2 * START_TIMEOUT,
            start_plugins(registry, &config(None, vec![None])),
        )
        .await
        .expect("start ran without a timeout");
        assert!(matches!(result, Err(Error::BadConfig)));
        // Paused time advances only as far as the timeout.
        assert_eq!(before.elapsed(), START_TIMEOUT);
        // `third` never starts; `second` isn't shut down, as it didn't start.
        assert_eq!(
            events(&recorded),
            [
                "start first false",
                "start second false",
                "shutdown first",
                "shut down first",
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_plugin_that_hangs_on_shutdown_is_left_after_the_timeout() {
        let (registry, recorded) = registry(vec![
            ("first", Behavior::Starts),
            ("second", Behavior::HangsOnShutdown),
        ]);
        let started = start_plugins(registry, &config(None, vec![]))
            .await
            .unwrap();
        let before = tokio::time::Instant::now();
        started.shutdown().await;
        // Paused time advances only as far as the timeout.
        assert_eq!(before.elapsed(), SHUTDOWN_TIMEOUT);
        assert_eq!(
            events(&recorded)[2..],
            ["shutdown second", "shutdown first", "shut down first"]
        );
    }

    #[tokio::test]
    async fn no_configured_plugins_start_nothing() {
        let (registry, _) = registry(vec![]);
        let started = start_plugins(registry, &config(None, vec![None]))
            .await
            .unwrap();
        assert_eq!(started.count, 0);
        started.shutdown().await;
    }

    #[test]
    fn every_plugin_adds_its_metrics() {
        let (registry, _) = registry(vec![
            ("first", Behavior::Starts),
            ("second", Behavior::FailsToStart),
        ]);
        let mut lines = vec!["pgcat_before 1".to_string()];
        registry_lines(&registry, &mut lines);
        assert_eq!(
            lines,
            ["pgcat_before 1", "pgcat_first_up 1", "pgcat_second_up 1"]
        );
        // The process registry is empty in unit tests.
        let mut lines = Vec::new();
        prometheus_lines(&mut lines);
        assert!(lines.is_empty());
    }

    /// Lines a plugin named NAME adds, the valid ones first.
    const BAD_LINES: &[&str] = &[
        r"# HELP pgcat_NAME_ok A metric of the plugin's: a \\ and a\nline",
        "# TYPE pgcat_NAME_ok gauge",
        r#"pgcat_NAME_ok{kind="a\"b\\c\nd",shard="0",} 1"#,
        "pgcat_NAME_ok{} -2.5e3 1700000000000",
        "pgcat_NAME_ok NaN",
        r#"pgcat_NAME_ok{kind="up"} +Inf"#,
        r#"pgcat_NAME_ok{kind="down"} -Inf"#,
        r#"pgcat_NAME_ok{kind="max"} 1.7976931348623157e308 0"#,
        // Not its own: core's, another plugin's, or no name after the prefix.
        "pgcat_pools_x 1",
        "# HELP pgcat_other_up Another plugin's",
        "pgcat_NAME_ 1",
        // More than one line, or not valid.
        "pgcat_NAME_a 1\npgcat_pools_y 2",
        "pgcat_NAME_a 1\r",
        r"# HELP pgcat_NAME_a An \x escape",
        r"# HELP pgcat_NAME_a A lone backslash \",
        "# TYPE pgcat_NAME_a gauges",
        "# TYPE pgcat_NAME_a",
        "# a comment",
        "pgcat_NAME_a",
        "pgcat_NAME_a one",
        "pgcat_NAME_a 1 2 3",
        "pgcat_NAME_a 1 later",
        "pgcat_NAME_a 1 -1",
        "pgcat_NAME_a 1 +1",
        "pgcat_NAME_a 1e9999",
        "pgcat_NAME_a -1e9999",
        "pgcat_NAME_a inf",
        "pgcat_NAME_a  1",
        "pgcat_NAME-a 1",
        r#"pgcat_NAME_a{kind="a} 1"#,
        r#"pgcat_NAME_a{kind="a"shard="0"} 1"#,
        r#"pgcat_NAME_a{kind="\x"} 1"#,
        r#"pgcat_NAME_a{0kind="a"} 1"#,
        r#"pgcat_NAME_a{kind=a} 1"#,
        r#"pgcat_NAME_a{kind="a",kind="b"} 1"#,
        r#"pgcat_NAME_a{__name__="pgcat_pools_x"} 1"#,
        r#"pgcat_NAME_a{kind="a"}1"#,
    ];

    #[test]
    fn a_plugins_bad_metric_lines_are_dropped() {
        let (registry, _) = registry(vec![
            ("first", Behavior::AddsBadLines),
            ("second", Behavior::Starts),
        ]);
        let mut lines = Vec::new();
        registry_lines(&registry, &mut lines);
        let mut kept: Vec<String> = BAD_LINES[..8]
            .iter()
            .map(|line| line.replace("NAME", "first"))
            .collect();
        kept.push("pgcat_first_up 1".into());
        // The other plugin's lines aren't affected.
        kept.push("pgcat_second_up 1".into());
        assert_eq!(lines, kept);
    }
}
