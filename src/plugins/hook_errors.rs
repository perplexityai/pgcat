//! `pgcat_hook_errors_total`: the plugin hook errors clients got, by
//! plugin, hook and the SQLSTATE the client got: a hook's error, timeout
//! (PH001) or missing slot (PH002), and a verifier's refusal: 28000 for a
//! `SET PGCAT_IDENTITY`, 0A000 for an identity verified again after a reload
//! changed its verifier, which ends the session.
//! Plugins' own decisions (`Deny`) and core's refusals aren't errors, so
//! they aren't counted.
use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::HOOK_BUSY;
use crate::messages::sqlstate_of;

/// Where a plugin's error reached the client: the `hook` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Hook {
    /// `SET PGCAT_IDENTITY`, or the identity a client holds verified again
    /// after a reload changed its verifier: the verifier refused the token,
    /// or didn't answer in time.
    VerifyIdentity,
    /// The plugin chain on a simple Query: counted for the plugin that
    /// failed, or was running when the chain timed out.
    Query,
    /// The plugin chain on a Parse, the same way.
    Parse,
    /// A planned statement's `on_execute_query`, counted for the plugin that
    /// planned it, like the plan's other hooks.
    Execute,
    /// A planned statement's `before_checkout`.
    Checkout,
    /// A planned statement's `on_bind`.
    Bind,
    /// A planned statement's `on_param_description`.
    ParamDescription,
    /// A planned statement's `on_row_description`.
    RowDescription,
    /// A planned statement's `on_rows`.
    Rows,
}

impl Hook {
    fn label(self) -> &'static str {
        match self {
            Hook::VerifyIdentity => "verify_identity",
            Hook::Query => "query",
            Hook::Parse => "parse",
            Hook::Execute => "execute",
            Hook::Checkout => "checkout",
            Hook::Bind => "bind",
            Hook::ParamDescription => "param_description",
            Hook::RowDescription => "row_description",
            Hook::Rows => "rows",
        }
    }
}

/// The `plugin` label of an error no plugin ran for: PH002, which a hook
/// gets before it runs, or a statement no plugin is known to have planned.
pub(crate) const NO_PLUGIN: &str = "none";

/// The SQLSTATE label of an error whose code isn't five of `[0-9A-Z]`.
const OTHER_SQLSTATE: &[u8; 5] = b"XX000";

/// Every label is a registered plugin's `&'static str` name, a fixed one,
/// or a SQLSTATE of five `[0-9A-Z]`: never a message, a token or a tenant.
type Labels = (&'static str, Hook, [u8; 5]);

static ERRORS: Lazy<Mutex<BTreeMap<Labels, u64>>> = Lazy::new(Default::default);

/// Count `error`, the message the client gets with its SQLSTATE, from
/// `plugin`'s `hook`. A PH002 counts for no plugin: none ran.
pub(crate) fn count(plugin: &'static str, hook: Hook, error: &str) {
    count_code(plugin, hook, sqlstate_of(error).0);
}

/// Count an error the client gets with SQLSTATE `code`.
pub(crate) fn count_code(plugin: &'static str, hook: Hook, code: Option<&str>) {
    let sqlstate = sqlstate_label(code);
    let plugin = if HOOK_BUSY.as_bytes() == sqlstate {
        NO_PLUGIN
    } else {
        plugin
    };
    *ERRORS.lock().entry((plugin, hook, sqlstate)).or_default() += 1;
}

fn sqlstate_label(code: Option<&str>) -> [u8; 5] {
    match code.map(str::as_bytes) {
        Some(&[a, b, c, d, e])
            if [a, b, c, d, e]
                .iter()
                .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()) =>
        {
            [a, b, c, d, e]
        }
        _ => *OTHER_SQLSTATE,
    }
}

/// The counter's lines for the Prometheus `/metrics` page.
pub(crate) fn prometheus_lines() -> Vec<String> {
    let errors = ERRORS.lock();
    let mut lines = Vec::with_capacity(errors.len() + 2);
    lines.push(
        "# HELP pgcat_hook_errors_total Plugin hook errors clients got, by plugin, \
         hook and SQLSTATE"
            .into(),
    );
    lines.push("# TYPE pgcat_hook_errors_total counter".into());
    for (&(plugin, hook, sqlstate), count) in errors.iter() {
        lines.push(format!(
            "pgcat_hook_errors_total{{plugin=\"{}\",hook=\"{}\",sqlstate=\"{}\"}} {}",
            label_value(plugin),
            hook.label(),
            String::from_utf8_lossy(&sqlstate),
            count
        ));
    }
    lines
}

/// A plugin's name as a label value: any `&'static str` registers.
fn label_value(name: &str) -> String {
    name.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The count for these labels, for tests.
#[cfg(test)]
pub(crate) fn count_of(plugin: &'static str, hook: Hook, sqlstate: &str) -> u64 {
    let sqlstate = sqlstate_label(Some(sqlstate));
    ERRORS
        .lock()
        .get(&(plugin, hook, sqlstate))
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::with_sqlstate;

    #[test]
    fn labels_are_bounded() {
        let plugin = "hook_errors_labels_test";
        count(
            plugin,
            Hook::Bind,
            &with_sqlstate("PH001", "a plugin hook took too long"),
        );
        // Not five of [0-9A-Z]: counted as XX000, never as the plugin's text.
        for bogus in ["ph001", "PH0011", "PH01", "PH 01", "PH\u{e9}1", ""] {
            count(plugin, Hook::Bind, &with_sqlstate(bogus, "a bogus code"));
        }
        count(plugin, Hook::Bind, "no code at all");
        assert_eq!(count_of(plugin, Hook::Bind, "PH001"), 1);
        assert_eq!(count_of(plugin, Hook::Bind, "XX000"), 7);
        // PH002: no plugin ran.
        let busy = count_of(NO_PLUGIN, Hook::Rows, "PH002");
        count(
            plugin,
            Hook::Rows,
            &with_sqlstate("PH002", "too many plugin hooks"),
        );
        assert_eq!(count_of(plugin, Hook::Rows, "PH002"), 0);
        assert_eq!(count_of(NO_PLUGIN, Hook::Rows, "PH002"), busy + 1);

        let lines = prometheus_lines();
        assert_eq!(lines[1], "# TYPE pgcat_hook_errors_total counter");
        let ours: Vec<_> = lines
            .iter()
            .filter(|line| line.contains(plugin))
            .map(String::as_str)
            .collect();
        assert_eq!(
            ours,
            [
                "pgcat_hook_errors_total{plugin=\"hook_errors_labels_test\",hook=\"bind\",sqlstate=\"PH001\"} 1",
                "pgcat_hook_errors_total{plugin=\"hook_errors_labels_test\",hook=\"bind\",sqlstate=\"XX000\"} 7",
            ]
        );
    }

    #[test]
    fn a_plugin_name_is_escaped() {
        count(
            "hook\"errors\\name\ntest",
            Hook::Query,
            &with_sqlstate("XX000", "x"),
        );
        assert!(prometheus_lines().iter().any(|line| line
            == "pgcat_hook_errors_total{plugin=\"hook\\\"errors\\\\name\\ntest\",hook=\"query\",sqlstate=\"XX000\"} 1"));
    }
}
