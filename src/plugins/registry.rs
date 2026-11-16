//! Static extensions supplied by the embedding executable: process-wide
//! plugins, and plugins pool TOML turns on with their own `[plugins.<name>]`.
use std::sync::Arc;

use async_trait::async_trait;
use once_cell::sync::{Lazy, OnceCell};
use sqlparser::ast::Statement;

use super::{PluginContext, PluginResult};
use crate::errors::Error;

mod configured;

pub(crate) use configured::{not_loaded, DynConfigured, Section, NOT_LOADED};
pub use configured::{ConfiguredPlugin, PluginSection};

/// Shared plugin state must be immutable or internally synchronized. Caller and
/// statement state belongs in the context and returned StatementPlan, not here.
#[async_trait]
pub trait RegisteredPlugin: Send + Sync {
    async fn on_query(
        &self,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error>;

    /// The result is kept for the prepared statement and not asked for again
    /// when the client's identity changes: it mustn't depend on the identity.
    async fn on_parse(
        &self,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        self.on_query(context, statements).await
    }
}

/// Registration order is execution order. The runner consumes the registry
/// before accepting connections; config reload cannot replace its code/state.
#[derive(Default)]
pub struct PluginRegistry {
    entries: Vec<(&'static str, Box<dyn RegisteredPlugin>)>,
    configured: Vec<(&'static str, Box<dyn DynConfigured>)>,
}

/// `[plugins.<name>]` sections core parses itself: a configured plugin can't
/// have these names. They're the fields of `config::Plugins`, which a test
/// checks.
const BUILT_IN_SECTIONS: &[&str] = &[
    "intercept",
    "table_access",
    "query_logger",
    "prewarmer",
    "testing",
];

/// Core's metric families: `pgcat_<family>_...`. A configured plugin's are
/// `pgcat_<name>_...`, so no name can share metrics with one, or with
/// another plugin's: `a` would include `a_b`'s. An end-to-end test checks
/// these cover every family `/metrics` serves.
const CORE_METRICS: &[&str] = &["databases", "servers", "stats", "pools", "sql", "hook"];

/// `pgcat_<a>_...` and `pgcat_<b>_...` can name the same metric.
fn metrics_overlap(a: &str, b: &str) -> bool {
    let within = |a: &str, b: &str| {
        a.strip_prefix(b)
            .map_or(false, |rest| rest.is_empty() || rest.starts_with('_'))
    };
    within(a, b) || within(b, a)
}

impl PluginRegistry {
    pub fn register(
        &mut self,
        name: &'static str,
        plugin: impl RegisteredPlugin + 'static,
    ) -> Result<(), Error> {
        if !self.name_available(name) {
            return Err(Error::BadConfig);
        }
        self.entries.push((name, Box::new(plugin)));
        Ok(())
    }

    /// Register a plugin that pools turn on with a `[plugins.<name>]` section.
    /// The name must be unique among all registered plugins and not one of
    /// core's own plugin sections, and its metrics' `pgcat_<name>_` prefix
    /// can't overlap core's or another configured plugin's.
    pub fn register_configured(
        &mut self,
        name: &'static str,
        plugin: impl ConfiguredPlugin,
    ) -> Result<(), Error> {
        let overlaps = CORE_METRICS
            .iter()
            .copied()
            .chain(self.configured.iter().map(|(other, _)| *other))
            .any(|other| metrics_overlap(name, other));
        if !self.name_available(name) || BUILT_IN_SECTIONS.contains(&name) || overlaps {
            return Err(Error::BadConfig);
        }
        self.configured
            .push((name, Box::new(configured::Typed(plugin))));
        Ok(())
    }

    fn name_available(&self, name: &str) -> bool {
        !name.is_empty()
            && !self
                .entries
                .iter()
                .map(|(registered, _)| registered)
                .chain(self.configured.iter().map(|(registered, _)| registered))
                .any(|registered| *registered == name)
    }

    /// Nothing is registered: the executable behaves like the standalone one.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.configured.is_empty()
    }

    /// Process-wide plugins are registered: they see every pool's statements.
    pub(crate) fn has_process_plugins(&self) -> bool {
        !self.entries.is_empty()
    }

    pub(crate) fn plugins(&self) -> impl Iterator<Item = (&'static str, &dyn RegisteredPlugin)> {
        self.entries
            .iter()
            .map(|(name, plugin)| (*name, plugin.as_ref()))
    }

    /// Configured plugins, in registration order.
    pub(crate) fn configured_plugins(
        &self,
    ) -> impl Iterator<Item = (&'static str, &dyn DynConfigured)> {
        self.configured
            .iter()
            .map(|(name, plugin)| (*name, plugin.as_ref()))
    }

    pub(crate) fn configured_plugin(&self, name: &str) -> Option<&dyn DynConfigured> {
        self.configured
            .iter()
            .find(|(registered, _)| *registered == name)
            .map(|(_, plugin)| plugin.as_ref())
    }
}

static PROCESS_PLUGINS: OnceCell<Arc<PluginRegistry>> = OnceCell::new();
static NO_PLUGINS: Lazy<Arc<PluginRegistry>> = Lazy::new(|| Arc::new(PluginRegistry::default()));

pub(crate) fn install(plugins: PluginRegistry) -> Result<(), Error> {
    PROCESS_PLUGINS
        .set(Arc::new(plugins))
        .map_err(|_| Error::BadConfig)
}

pub(crate) fn process_plugins() -> Arc<PluginRegistry> {
    Arc::clone(PROCESS_PLUGINS.get().unwrap_or(&NO_PLUGINS))
}

#[cfg(test)]
mod configured_tests;
#[cfg(test)]
mod tests;
