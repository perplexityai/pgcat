use super::*;
use crate::config::{Config, Plugins, Pool, TableAccess};
use crate::identity::{IdentityToken, VerifiedIdentity};
use crate::messages::{simple_query, sqlstate_of};
use crate::plugins::hook_errors::{count_of, Hook, NO_PLUGIN};
use crate::plugins::{statements_to_sql, PluginOutput, PLUGIN_FAILED};
use crate::pool::PoolSettings;
use crate::query_router::{QueryRouter, Verifying};
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SyntheticConfig {
    #[serde(default)]
    enabled: bool,
    /// `SELECT 1` is rewritten to `SELECT <answer>`.
    #[serde(default)]
    answer: Option<i64>,
    #[serde(default)]
    tables: Vec<String>,
    /// Allow unparsed queries that name none of `tables`.
    #[serde(default)]
    table_scoped: bool,
    #[serde(default)]
    sensitive: bool,
    #[serde(default)]
    withhold: bool,
    #[serde(default)]
    needs_parse: bool,
    /// Never shown: the tests check logs for it.
    #[serde(default)]
    #[allow(dead_code)]
    note: String,
}

#[derive(Default)]
struct Seen {
    calls: AtomicUsize,
    parses: AtomicUsize,
    generation: AtomicU64,
}

struct Synthetic(Arc<Seen>);

#[async_trait]
impl ConfiguredPlugin for Synthetic {
    type Config = SyntheticConfig;

    fn enabled(&self, config: &SyntheticConfig) -> bool {
        config.enabled
    }

    fn validate(&self, config: &SyntheticConfig, pool: &Pool) -> Result<(), String> {
        if config.enabled && config.needs_parse && !pool.plugins_require_parse {
            return Err("the synthetic plugin needs plugins_require_parse".into());
        }
        Ok(())
    }

    async fn on_query(
        &self,
        config: &SyntheticConfig,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0
            .generation
            .store(context.plan_generation(), Ordering::SeqCst);
        match (config.answer, statements) {
            (Some(answer), [statement]) if statement.to_string() == "SELECT 1" => {
                Ok(PluginOutput::Overwrite(parse(&format!("SELECT {}", answer))).into())
            }
            _ => Ok(PluginOutput::Allow.into()),
        }
    }

    async fn on_parse(
        &self,
        config: &SyntheticConfig,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        assert_eq!(context.declared_param_types, [23]);
        self.0.parses.fetch_add(1, Ordering::SeqCst);
        self.on_query(config, context, statements).await
    }

    fn sensitive_queries(&self, config: &SyntheticConfig) -> bool {
        config.sensitive
    }

    fn withholds_server_text(&self, config: &SyntheticConfig) -> bool {
        config.withhold
    }

    fn allows_unparsed_outside_gated_tables(&self, config: &SyntheticConfig) -> bool {
        config.table_scoped
    }

    fn gated_tables<'c>(&self, config: &'c SyntheticConfig) -> Vec<&'c str> {
        config.tables.iter().map(String::as_str).collect()
    }
}

fn parse(sql: &str) -> Vec<Statement> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap()
}

fn registry() -> (PluginRegistry, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let mut registry = PluginRegistry::default();
    registry
        .register_configured("synthetic", Synthetic(seen.clone()))
        .unwrap();
    (registry, seen)
}

/// `[plugins]` as written, before validation.
fn plugins(toml: &str) -> Plugins {
    toml::from_str(toml).unwrap()
}

/// `[plugins]` loaded by `registry`, as validation leaves them.
fn loaded(registry: &PluginRegistry, toml: &str) -> Plugins {
    let mut plugins = plugins(toml);
    registry.load_sections(&mut plugins).unwrap();
    plugins
}

fn router(registry: Arc<PluginRegistry>, plugins: Option<Plugins>) -> QueryRouter {
    let mut router = QueryRouter::with_plugins(registry);
    router.update_pool_settings(&PoolSettings {
        query_parser_enabled: true,
        plugins_require_parse: true,
        plugins,
        ..PoolSettings::default()
    });
    router
}

fn hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn registration_rejects_empty_duplicate_and_built_in_names() {
    let (mut registry, seen) = registry();
    for name in [
        "",
        "synthetic",
        "intercept",
        "table_access",
        "query_logger",
        "prewarmer",
        "testing",
    ] {
        assert_eq!(
            registry.register_configured(name, Synthetic(seen.clone())),
            Err(Error::BadConfig),
            "{:?}",
            name
        );
    }

    // One name space for both kinds of plugins.
    struct Process;
    #[async_trait]
    impl RegisteredPlugin for Process {
        async fn on_query(
            &self,
            _context: &PluginContext<'_>,
            _statements: &[Statement],
        ) -> Result<PluginResult, Error> {
            Ok(PluginOutput::Allow.into())
        }
    }
    assert_eq!(
        registry.register("synthetic", Process),
        Err(Error::BadConfig)
    );
    registry.register("process", Process).unwrap();
    assert_eq!(
        registry.register_configured("process", Synthetic(seen)),
        Err(Error::BadConfig)
    );
    assert_eq!(registry.configured_plugins().count(), 1);
}

#[test]
fn a_configured_plugin_name_does_not_share_metrics() {
    let (mut registry, seen) = registry();
    registry
        .register_configured("a_b", Synthetic(seen.clone()))
        .unwrap();
    // `pgcat_<name>_...` would include core's metrics, or another plugin's,
    // or be included in theirs.
    for name in [
        "pools",
        "sql",
        "sql_observer",
        "hook",
        "a",
        "a_b_c",
        "synthetic_x",
    ] {
        assert_eq!(
            registry.register_configured(name, Synthetic(seen.clone())),
            Err(Error::BadConfig),
            "{:?}",
            name
        );
    }
    registry
        .register_configured("ab", Synthetic(seen.clone()))
        .unwrap();
    registry
        .register_configured("poolside", Synthetic(seen))
        .unwrap();
}

#[test]
fn a_configured_plugin_alone_is_not_process_wide() {
    let (registry, _) = registry();
    assert!(!registry.is_empty());
    assert!(!registry.has_process_plugins());
    assert!(PluginRegistry::default().is_empty());
}

#[test]
fn built_in_sections_parse_as_before_and_the_rest_are_kept_as_written() {
    let plugins = plugins(
        r#"
        [table_access]
        enabled = true
        tables = ["blocked"]

        [synthetic]
        enabled = true
        answer = 5
        "#,
    );
    assert_eq!(
        plugins.table_access,
        Some(TableAccess {
            enabled: true,
            tables: vec!["blocked".into()],
        })
    );
    assert_eq!(
        plugins.configured.keys().collect::<Vec<_>>(),
        vec!["synthetic"]
    );
    assert_eq!(
        plugins.configured["synthetic"].raw()["answer"].as_integer(),
        Some(5)
    );
    // Not parsed until the config is validated.
    assert!(plugins.configured["synthetic"].parsed().is_none());
}

#[test]
fn a_section_without_its_plugin_fails_the_pool() {
    let (registry, _) = registry();
    let mut pool = Pool {
        query_parser_enabled: true,
        plugins: Some(plugins("[unknown]\nenabled = true")),
        ..Default::default()
    };
    assert_eq!(
        pool.validate_with_plugins(None, &registry),
        Err(Error::BadConfig)
    );
    // Nor does a binary without plugins run it as if it were off.
    assert_eq!(
        pool.validate_with_plugins(None, &PluginRegistry::default()),
        Err(Error::BadConfig)
    );
    // The process registry is empty in unit tests.
    assert_eq!(pool.validate(None), Err(Error::BadConfig));
}

#[test]
fn an_unknown_global_section_fails_even_if_no_pool_inherits_it() {
    let (registry, _) = registry();
    let mut config = Config {
        plugins: Some(plugins("[unknown]\nenabled = false")),
        ..Default::default()
    };
    config.pools.insert(
        "db".into(),
        Pool {
            query_parser_enabled: true,
            plugins: Some(Plugins::default()),
            ..Default::default()
        },
    );
    assert_eq!(
        config.validate_with_plugins(&registry),
        Err(Error::BadConfig)
    );
    config.plugins = Some(plugins("[synthetic]\nenabled = false"));
    assert_eq!(config.validate_with_plugins(&registry), Ok(()));
}

#[test]
fn a_section_its_plugin_cannot_parse_fails_the_pool() {
    let (registry, _) = registry();
    for section in [
        "[synthetic]\nenabled = \"yes\"",
        // deny_unknown_fields works through the section as written.
        "[synthetic]\nenabled = true\nanswr = 5",
        "synthetic = 1",
    ] {
        let mut pool = Pool {
            query_parser_enabled: true,
            plugins: Some(plugins(section)),
            ..Default::default()
        };
        assert_eq!(
            pool.validate_with_plugins(None, &registry),
            Err(Error::BadConfig),
            "{}",
            section
        );
    }
}

#[test]
fn the_plugin_checks_the_pool_its_section_applies_to() {
    let (registry, _) = registry();
    let section = "[synthetic]\nenabled = true\nneeds_parse = true";
    let mut pool = Pool {
        query_parser_enabled: true,
        plugins_require_parse: false,
        plugins: Some(plugins(section)),
        ..Default::default()
    };
    assert_eq!(
        pool.validate_with_plugins(None, &registry),
        Err(Error::BadConfig)
    );
    pool.plugins_require_parse = true;
    assert_eq!(pool.validate_with_plugins(None, &registry), Ok(()));
    assert!(pool.plugins.as_ref().unwrap().configured["synthetic"]
        .parsed()
        .is_some());

    // Inherited, the global section is checked against each pool.
    let mut config = Config {
        plugins: Some(plugins(section)),
        ..Default::default()
    };
    config.pools.insert(
        "db".into(),
        Pool {
            query_parser_enabled: true,
            plugins_require_parse: false,
            ..Default::default()
        },
    );
    assert_eq!(
        config.validate_with_plugins(&registry),
        Err(Error::BadConfig)
    );
    config.pools.get_mut("db").unwrap().plugins_require_parse = true;
    assert_eq!(config.validate_with_plugins(&registry), Ok(()));
    assert!(config.plugins.as_ref().unwrap().configured["synthetic"]
        .parsed()
        .is_some());
}

#[test]
fn an_enabled_plugin_needs_pools_that_run_plugins() {
    let (registry, _) = registry();
    let mut config = Config {
        plugins: Some(plugins("[synthetic]\nenabled = true")),
        ..Default::default()
    };
    config.pools.insert(
        "db".into(),
        Pool {
            query_parser_enabled: false,
            ..Default::default()
        },
    );
    assert_eq!(
        config.validate_with_plugins(&registry),
        Err(Error::BadConfig)
    );
    config.plugins = Some(plugins("[synthetic]\nenabled = false"));
    assert_eq!(config.validate_with_plugins(&registry), Ok(()));
}

#[test]
fn logs_show_whether_a_plugin_is_on_never_its_settings() {
    let (registry, _) = registry();
    let unparsed = plugins("[synthetic]\nenabled = true\nnote = \"sentinel-value\"");
    assert!(unparsed
        .to_string()
        .ends_with(", \"synthetic\": not loaded"));
    let parsed = loaded(
        &registry,
        "[synthetic]\nenabled = true\nnote = \"sentinel-value\"",
    );
    assert!(parsed.to_string().ends_with(", \"synthetic\": true"));
    for shown in [
        parsed.to_string(),
        format!("{:?}", parsed),
        format!("{:#?}", Some(parsed)),
    ] {
        assert!(!shown.contains("sentinel-value"), "{}", shown);
    }
}

#[test]
fn sections_compare_and_hash_as_written() {
    let (registry, _) = registry();
    let section = "[synthetic]\nenabled = true\nanswer = 5";
    let written = plugins(section);
    let parsed = loaded(&registry, section);
    assert_eq!(written, parsed);
    assert_eq!(hash(&written), hash(&parsed));

    let other = plugins("[synthetic]\nenabled = true\nanswer = 6");
    assert_ne!(written, other);
    assert_ne!(hash(&written), hash(&other));

    let zero = PluginSection::new(toml::Value::Float(0.0));
    let negative_zero = PluginSection::new(toml::Value::Float(-0.0));
    assert_eq!(zero, negative_zero);
    assert_eq!(hash(&zero), hash(&negative_zero));
}

#[tokio::test]
async fn runs_only_on_pools_that_turn_it_on() {
    let (registry, seen) = registry();
    let registry = Arc::new(registry);

    let on = router(
        registry.clone(),
        Some(loaded(&registry, "[synthetic]\nenabled = true\nanswer = 5")),
    );
    assert_eq!(
        on.execute_query_plugins(&parse("SELECT 1"), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Overwrite(parse("SELECT 5"))
    );
    assert_eq!(seen.calls.load(Ordering::SeqCst), 1);

    let off = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[synthetic]\nenabled = false\nanswer = 5",
        )),
    );
    let without = router(registry.clone(), Some(Plugins::default()));
    let none = router(registry.clone(), None);
    for router in [&off, &without, &none] {
        assert_eq!(
            router
                .execute_query_plugins(&parse("SELECT 1"), None)
                .await
                .unwrap()
                .output,
            PluginOutput::Allow
        );
        assert_eq!(router.plan_generation(), 0);
        assert!(!router.redact_queries());
        assert!(!router.withholds_server_text());
    }
    assert_eq!(seen.calls.load(Ordering::SeqCst), 1);

    // Unlike a process-wide plugin, it doesn't make other pools parse every
    // statement and refuse what the parser can't read.
    let mut parser_off = QueryRouter::with_plugins(registry.clone());
    parser_off.update_pool_settings(&PoolSettings::default());
    assert!(!parser_off.plugins_enabled());
    assert!(!parser_off.plugins_require_parse());
    for router in [&parser_off, &without, &off] {
        assert!(!router.rejects_unparsed(&simple_query("not valid sql")));
    }
}

#[tokio::test]
async fn parse_gets_the_declared_types() {
    let (registry, seen) = registry();
    let registry = Arc::new(registry);
    let on = router(
        registry.clone(),
        Some(loaded(&registry, "[synthetic]\nenabled = true")),
    );
    on.execute_parse_plugins(&parse("SELECT $1"), &[23], None)
        .await
        .unwrap();
    assert_eq!(seen.parses.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn runs_after_the_built_ins() {
    let (registry, seen) = registry();
    let registry = Arc::new(registry);
    let mut plugins = loaded(&registry, "[synthetic]\nenabled = true\nanswer = 5");
    plugins.table_access = Some(TableAccess {
        enabled: true,
        tables: vec!["blocked".into()],
    });
    let on = router(registry, Some(plugins));
    assert!(matches!(
        on.execute_query_plugins(&parse("SELECT * FROM blocked"), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Deny(_)
    ));
    assert_eq!(seen.calls.load(Ordering::SeqCst), 0);

    // table_access still checks the final rewrite.
    let registry = registry_with_rewrite_to("SELECT * FROM blocked");
    let registry = Arc::new(registry);
    let mut plugins = loaded(&registry, "[rewriter]\nenabled = true");
    plugins.table_access = Some(TableAccess {
        enabled: true,
        tables: vec!["blocked".into()],
    });
    let on = router(registry, Some(plugins));
    assert_eq!(
        on.execute_query_plugins(&parse("SELECT 1"), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Deny(crate::messages::with_sqlstate(
            crate::plugins::PLUGIN_DENIED,
            "permission denied for rewritten statement",
        ))
    );
}

struct Rewriter(&'static str);

#[derive(Deserialize)]
struct RewriterConfig {
    enabled: bool,
}

#[async_trait]
impl ConfiguredPlugin for Rewriter {
    type Config = RewriterConfig;

    fn enabled(&self, config: &RewriterConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        _config: &RewriterConfig,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginOutput::Overwrite(parse(self.0)).into())
    }
}

fn registry_with_rewrite_to(sql: &'static str) -> PluginRegistry {
    let mut registry = PluginRegistry::default();
    registry
        .register_configured("rewriter", Rewriter(sql))
        .unwrap();
    registry
}

#[tokio::test]
async fn two_configured_rewrites_are_refused() {
    let mut registry = registry_with_rewrite_to("SELECT 2");
    registry
        .register_configured("other", Rewriter("SELECT 3"))
        .unwrap();
    let registry = Arc::new(registry);
    let on = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[rewriter]\nenabled = true\n[other]\nenabled = true",
        )),
    );
    assert_eq!(
        on.execute_query_plugins(&parse("SELECT 1"), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Deny("query rewritten by more than one plugin".into())
    );
}

#[tokio::test]
async fn a_section_no_plugin_parsed_fails_closed() {
    let (registry, seen) = registry();
    let registry = Arc::new(registry);

    // Built without validation, or parsed by another binary's plugins.
    let unparsed = router(
        registry.clone(),
        Some(plugins("[synthetic]\nenabled = false")),
    );
    let other = registry_with_rewrite_to("SELECT 2");
    let unregistered = router(
        Arc::new(PluginRegistry::default()),
        Some(loaded(&registry, "[synthetic]\nenabled = false")),
    );
    let wrong_type = router(
        Arc::new(other),
        Some({
            let mut plugins = loaded(&registry, "[synthetic]\nenabled = false");
            let section = plugins.configured.remove("synthetic").unwrap();
            plugins.configured.insert("rewriter".into(), section);
            plugins
        }),
    );

    for router in [&unparsed, &unregistered, &wrong_type] {
        let output = router
            .execute_query_plugins(&parse("SELECT 1"), None)
            .await
            .unwrap()
            .output;
        match output {
            PluginOutput::Deny(message) => {
                assert_eq!(sqlstate_of(&message).0, Some(PLUGIN_FAILED))
            }
            output => panic!("{:?}", output),
        }
        assert!(router.rejects_unparsed(&simple_query("not valid sql")));
        assert!(router.redact_queries());
        assert!(router.withholds_server_text());
        assert_ne!(router.plan_generation(), 0);
    }
    assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn plan_generation_follows_the_sections_of_plugins_that_are_on() {
    let (registry, seen) = registry();
    let registry = Arc::new(registry);
    let generation = |toml: &str| router(registry.clone(), Some(loaded(&registry, toml)));

    let five = generation("[synthetic]\nenabled = true\nanswer = 5");
    assert_eq!(five.plan_generation() & 1, 1);
    assert_eq!(
        five.plan_generation(),
        generation("[synthetic]\nanswer = 5\nenabled = true").plan_generation()
    );
    assert_ne!(
        five.plan_generation(),
        generation("[synthetic]\nenabled = true\nanswer = 6").plan_generation()
    );
    assert_eq!(
        generation("[synthetic]\nenabled = false\nanswer = 5").plan_generation(),
        0
    );

    // Settings other than the plugins' don't change it.
    let mut router = five;
    let before = router.plan_generation();
    let mut settings = router.pool_settings().clone();
    settings.plugin_hook_timeout += std::time::Duration::from_millis(1);
    router.update_pool_settings(&settings);
    assert_eq!(router.plan_generation(), before);

    // Plugins see the generation they plan with.
    router
        .execute_query_plugins(&parse("SELECT 1"), None)
        .await
        .unwrap();
    assert_eq!(seen.generation.load(Ordering::SeqCst), before);
}

#[test]
fn sensitive_queries_and_gated_tables_apply_while_the_plugin_is_on() {
    let (registry, _) = registry();
    let registry = Arc::new(registry);
    let on = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[synthetic]\nenabled = true\nsensitive = true\ntables = [\"secret\"]\ntable_scoped = true",
        )),
    );
    assert!(on.redact_queries());
    assert!(on.rejects_unparsed(&simple_query("selec * from secret")));
    assert!(!on.rejects_unparsed(&simple_query("selec * from public_table")));

    let off = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[synthetic]\nenabled = false\nsensitive = true\ntables = [\"secret\"]",
        )),
    );
    assert!(!off.redact_queries());
    assert!(!off.rejects_unparsed(&simple_query("selec * from secret")));
}

/// A Parse of `query`, unnamed and without declared types.
fn parse_message(query: &str) -> BytesMut {
    let mut message = BytesMut::from(&b"P"[..]);
    message.put_i32(4 + query.len() as i32 + 2 + 2);
    message.put_u8(0);
    message.put_slice(query.as_bytes());
    message.put_u8(0);
    message.put_i16(0);
    message
}

#[test]
fn unparsed_queries_are_refused_unless_the_plugin_is_table_scoped() {
    let (registry, _) = registry();
    let registry = Arc::new(registry);
    let with = |toml: &str, plugins_require_parse: bool| {
        let mut router = QueryRouter::with_plugins(registry.clone());
        router.update_pool_settings(&PoolSettings {
            query_parser_enabled: true,
            plugins_require_parse,
            plugins: Some(loaded(&registry, toml)),
            ..PoolSettings::default()
        });
        router
    };
    let rejects = |router: &QueryRouter, query: &str| {
        let simple = router.rejects_unparsed(&simple_query(query));
        assert_eq!(
            simple,
            router.rejects_unparsed(&parse_message(query)),
            "{}",
            query
        );
        simple
    };
    let section = "[synthetic]\nenabled = true\ntables = [\"secret\"]";
    let scoped = format!("{}\ntable_scoped = true", section);
    // SQL that builds the name passes the table-scoped check.
    let builds_the_name = "DO $$ BEGIN EXECUTE 'UPDATE secr' || 'et SET a = 1'; END $$";

    // By default the pool sees no statement the plugin hasn't, whether or
    // not it sets plugins_require_parse.
    for router in [with(section, true), with(section, false)] {
        for query in [
            "selec * from secret",
            "selec * from public_table",
            "SELEKT 1",
            builds_the_name,
        ] {
            assert!(rejects(&router, query), "{}", query);
        }
        // RESET can't touch a table: it's sent, as before.
        for query in ["RESET ALL", "reset search_path;"] {
            assert!(!rejects(&router, query), "{}", query);
        }
    }

    // A table-scoped plugin refuses those naming its tables, and only with
    // plugins_require_parse.
    let table_scoped = with(&scoped, true);
    assert!(rejects(&table_scoped, "selec * from secret"));
    for query in [
        "selec * from public_table",
        "SELEKT 1",
        builds_the_name,
        "RESET ALL",
    ] {
        assert!(!rejects(&table_scoped, query), "{}", query);
    }
    assert!(!rejects(&with(&scoped, false), "selec * from secret"));

    // A plugin that's off refuses none.
    let off = with("[synthetic]\nenabled = false", true);
    assert!(!rejects(&off, "SELEKT 1"));

    // Without the parser, which the pool's validation requires, plugins don't
    // run and nothing is refused.
    let mut parser_off = QueryRouter::with_plugins(registry.clone());
    parser_off.update_pool_settings(&PoolSettings {
        plugins: Some(loaded(&registry, section)),
        ..PoolSettings::default()
    });
    assert!(!rejects(&parser_off, "SELEKT 1"));
}

#[test]
fn a_table_scoped_plugin_doesnt_lift_another_plugins_refusal_of_unparsed_queries() {
    // In either registration order, with a strict and a table-scoped plugin
    // both on, the strict one's rule holds.
    for (first, second) in [("strict", "scoped"), ("scoped", "strict")] {
        let mut registry = PluginRegistry::default();
        for name in [first, second] {
            registry
                .register_configured(name, Synthetic(Arc::default()))
                .unwrap();
        }
        let registry = Arc::new(registry);
        let with = |strict_on: bool, plugins_require_parse: bool| {
            let toml = format!(
                "[strict]\nenabled = {}\ntables = [\"secret\"]\n\
                 [scoped]\nenabled = true\ntables = [\"secret\"]\ntable_scoped = true",
                strict_on
            );
            let mut router = QueryRouter::with_plugins(registry.clone());
            router.update_pool_settings(&PoolSettings {
                query_parser_enabled: true,
                plugins_require_parse,
                plugins: Some(loaded(&registry, &toml)),
                ..PoolSettings::default()
            });
            router
        };
        let rejects = |router: &QueryRouter, query: &str| {
            let simple = router.rejects_unparsed(&simple_query(query));
            assert_eq!(
                simple,
                router.rejects_unparsed(&parse_message(query)),
                "{}",
                query
            );
            simple
        };

        for plugins_require_parse in [true, false] {
            let mixed = with(true, plugins_require_parse);
            for query in ["SELEKT 1", "selec * from secret"] {
                assert!(
                    rejects(&mixed, query),
                    "{} with {} registered first, plugins_require_parse = {}",
                    query,
                    first,
                    plugins_require_parse
                );
            }
            assert!(!rejects(&mixed, "RESET ALL"));
        }

        // With the strict one off, the table-scoped rule is back.
        let scoped_only = with(false, true);
        assert!(!rejects(&scoped_only, "SELEKT 1"), "{} first", first);
        assert!(
            rejects(&scoped_only, "selec * from secret"),
            "{} first",
            first
        );
        assert!(!rejects(&with(false, false), "SELEKT 1"), "{} first", first);
    }
}

#[test]
fn server_text_is_withheld_while_a_plugin_that_says_so_is_on() {
    let (registry, _) = registry();
    let registry = Arc::new(registry);
    let withholds = |toml: &str| {
        router(registry.clone(), Some(loaded(&registry, toml))).withholds_server_text()
    };

    assert!(withholds("[synthetic]\nenabled = true\nwithhold = true"));
    // By default, and while the plugin is off, the server's text is shown.
    assert!(!withholds("[synthetic]\nenabled = true"));
    assert!(!withholds("[synthetic]\nenabled = false\nwithhold = true"));
    // It's separate from keeping queries out of the logs.
    assert!(!withholds("[synthetic]\nenabled = true\nsensitive = true"));
}

#[test]
fn the_built_in_sections_are_the_built_in_plugins_fields() {
    // A section named after a field serde parses into it, so a configured
    // plugin of that name would never run.
    let mut fields: Vec<String> = match serde_json::to_value(Plugins::default()).unwrap() {
        serde_json::Value::Object(fields) => fields.keys().cloned().collect(),
        other => panic!("Plugins serialized as {:?}", other),
    };
    let mut built_in: Vec<String> = BUILT_IN_SECTIONS.iter().map(|s| s.to_string()).collect();
    fields.sort();
    built_in.sort();
    assert_eq!(fields, built_in);
}

struct Denier(&'static str);

#[async_trait]
impl ConfiguredPlugin for Denier {
    type Config = RewriterConfig;

    fn enabled(&self, config: &RewriterConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        _config: &RewriterConfig,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginOutput::Deny(self.0.into()).into())
    }
}

#[tokio::test]
async fn configured_plugins_run_in_registration_order() {
    // Of two that deny, the client sees the first registered, whatever the
    // order of their sections.
    for (first, second) in [("a", "b"), ("b", "a")] {
        let mut registry = PluginRegistry::default();
        registry.register_configured(first, Denier(first)).unwrap();
        registry
            .register_configured(second, Denier(second))
            .unwrap();
        let registry = Arc::new(registry);
        let on = router(
            registry.clone(),
            Some(loaded(
                &registry,
                "[a]\nenabled = true\n[b]\nenabled = true",
            )),
        );
        assert_eq!(
            on.execute_query_plugins(&parse("SELECT 1"), None)
                .await
                .unwrap()
                .output,
            PluginOutput::Deny(first.into())
        );
    }

    // A denial ends the chain whether a rewrite came before it or not.
    for rewrite_first in [true, false] {
        let mut registry = PluginRegistry::default();
        if rewrite_first {
            registry
                .register_configured("rewriter", Rewriter("SELECT 2"))
                .unwrap();
        }
        registry
            .register_configured("denier", Denier("denied"))
            .unwrap();
        if !rewrite_first {
            registry
                .register_configured("rewriter", Rewriter("SELECT 2"))
                .unwrap();
        }
        let registry = Arc::new(registry);
        let on = router(
            registry.clone(),
            Some(loaded(
                &registry,
                "[denier]\nenabled = true\n[rewriter]\nenabled = true",
            )),
        );
        assert_eq!(
            on.execute_query_plugins(&parse("SELECT 1"), None)
                .await
                .unwrap()
                .output,
            PluginOutput::Deny("denied".into())
        );
    }
}

/// Verifies `tenant:<name>` tokens on pools it's on, unless told not to.
struct Verifier;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierConfig {
    enabled: bool,
    #[serde(default = "yes")]
    verify: bool,
}

fn yes() -> bool {
    true
}

#[derive(PartialEq)]
struct Claims(String);

#[async_trait]
impl ConfiguredPlugin for Verifier {
    type Config = VerifierConfig;

    fn enabled(&self, config: &VerifierConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        _config: &VerifierConfig,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginOutput::Allow.into())
    }

    fn verifies_identity(&self, config: &VerifierConfig) -> bool {
        config.verify
    }

    async fn verify_identity(
        &self,
        _config: &VerifierConfig,
        token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        if let Some(code) = token.as_str().strip_prefix("code:") {
            return Err(crate::messages::with_sqlstate(
                code,
                "a refusal with a code",
            ));
        }
        // A code inside a code, as a forwarded pgcat error might carry.
        if let Some(codes) = token.as_str().strip_prefix("codes:") {
            let (outer, inner) = codes.split_once(':').unwrap();
            return Err(crate::messages::with_sqlstate(
                outer,
                &crate::messages::with_sqlstate(inner, "a refusal with two codes"),
            ));
        }
        // Quotes a v1. token, or a part of it, as the word after its last '.'
        // says, or gives a reason without it.
        let v1 = token.as_str().strip_prefix("v1.");
        if let Some((payload, quote)) = v1.and_then(|rest| rest.rsplit_once('.')) {
            return Err(match quote {
                "whole" => format!("unknown token {}", token.as_str()),
                "unprefixed" => format!("signature mismatch for {}", v1.unwrap()),
                "slice20" => format!("bad part {}", &payload[8..28]),
                "slice19" => format!("bad part {}", &payload[8..27]),
                "expired" => "token has expired".into(),
                _ => "token is for issuer https://auth.example.com/realms/acme".into(),
            });
        }
        // A plugin that shouldn't: core keeps the token from the client.
        if token.as_str().starts_with("echo:") {
            return Err(format!("unknown token {}", token.as_str()));
        }
        let tenant = token
            .as_str()
            .strip_prefix("tenant:")
            .ok_or("not a token of this plugin")?;
        Ok(VerifiedIdentity {
            tenant: Some(tenant.into()),
            expires_at: None,
            claims: Some(Arc::new(Claims(tenant.into()))),
        })
    }
}

fn verifiers() -> PluginRegistry {
    let mut registry = PluginRegistry::default();
    registry.register_configured("verifier", Verifier).unwrap();
    registry.register_configured("other", Verifier).unwrap();
    registry
        .register_configured("synthetic", Synthetic(Arc::default()))
        .unwrap();
    registry
}

fn token(token: &str) -> IdentityToken {
    IdentityToken::new(token.into())
}

#[tokio::test]
async fn the_pools_verifier_verifies_identities() {
    let registry = Arc::new(verifiers());
    let on = router(
        registry.clone(),
        Some(loaded(&registry, "[verifier]\nenabled = true")),
    );
    let verified = on.verify_identity(&token("tenant:acme")).await.unwrap();
    assert_eq!(verified.tenant.as_deref(), Some("acme"));
    let identity = crate::identity::Identity::new(token("tenant:acme"), verified);
    assert_eq!(
        identity.claims::<Claims>().map(|c| c.0.as_str()),
        Some("acme")
    );
    assert_eq!(
        on.verify_identity(&token("forged")).await.unwrap_err(),
        "not a token of this plugin"
    );
    // A refusal can't pick its SQLSTATE: the client gets 28000.
    assert_eq!(
        on.verify_identity(&token("code:XX000")).await.unwrap_err(),
        "a refusal with a code"
    );
    // Nor by nesting one in another: no NUL is left for client.rs to read
    // PH001 from, so the client still gets 28000.
    let nested = on
        .verify_identity(&token("codes:XX000:PH001"))
        .await
        .unwrap_err();
    assert_eq!(nested, " PH001 a refusal with two codes");
    assert_eq!(crate::messages::sqlstate_of(&nested).0, None);
    assert_eq!(
        crate::messages::sqlstate_of(&crate::messages::or_sqlstate(
            crate::plugins::IDENTITY_REFUSED,
            &format!("PGCAT_IDENTITY refused: {}", nested)
        ))
        .0,
        Some("28000")
    );
    // A refusal that quotes the token is replaced, also when no 20 characters
    // of it are in a row.
    let refused = on
        .verify_identity(&token("echo:secret-91d2"))
        .await
        .unwrap_err();
    assert!(!refused.contains("secret-91d2"), "{}", refused);
    assert_eq!(refused, "the plugin refused the identity");
    // So is one that quotes it without its v1., or 20 characters of it in a
    // row; 19 go through, and so do reasons without any.
    let payload = "eyJ0ZW5hbnQiOiJhY21lIiwia2V5IjoiU0VDUkVUIn0";
    for (quote, expected) in [
        ("whole", "the plugin refused the identity".to_string()),
        ("unprefixed", "the plugin refused the identity".to_string()),
        ("slice20", "the plugin refused the identity".to_string()),
        ("slice19", format!("bad part {}", &payload[8..27])),
        ("expired", "token has expired".to_string()),
        (
            "issuer",
            "token is for issuer https://auth.example.com/realms/acme".to_string(),
        ),
    ] {
        let token = token(&format!("v1.{}.{}", payload, quote));
        let refused = on.verify_identity(&token).await.unwrap_err();
        assert_eq!(refused, expected, "{}", quote);
    }

    // Pools it isn't on, or doesn't verify on, keep tokens as they are.
    for toml in [
        "[verifier]\nenabled = false",
        "[verifier]\nenabled = true\nverify = false",
        "[synthetic]\nenabled = true",
    ] {
        let router = router(registry.clone(), Some(loaded(&registry, toml)));
        let verified = router.verify_identity(&token("forged")).await.unwrap();
        assert_eq!(verified.tenant, None, "{}", toml);
    }
    // Even where plugins require an identity: one without claims isn't
    // verified, whatever the token.
    let mut required = QueryRouter::with_plugins(registry.clone());
    required.update_pool_settings(&PoolSettings {
        query_parser_enabled: true,
        plugins_require_parse: true,
        identity_required_for_plugins: true,
        plugins: Some(loaded(&registry, "[synthetic]\nenabled = true")),
        ..PoolSettings::default()
    });
    let kept = required.verify_identity(&token("forged")).await.unwrap();
    assert!(kept.tenant.is_none() && kept.claims.is_none());

    let none = router(registry, None);
    assert_eq!(
        none.verify_identity(&token("forged")).await.unwrap().tenant,
        None
    );
}

#[test]
fn a_pool_has_one_verifier_at_most() {
    let registry = verifiers();
    let pool = |toml: &str| Pool {
        query_parser_enabled: true,
        plugins_require_parse: true,
        identity_required_for_plugins: true,
        identity_tenant_parameter: Some("app.tenant".into()),
        plugins: Some(plugins(toml)),
        ..Default::default()
    };
    for toml in [
        "[verifier]\nenabled = true\n[other]\nenabled = true",
        "[verifier]\nenabled = true\n[testing]\nenabled = true\nverify_identity = true",
    ] {
        assert_eq!(
            pool(toml).validate_with_plugins(None, &registry),
            Err(Error::BadConfig),
            "{}",
            toml
        );
    }
    for toml in [
        "[verifier]\nenabled = true\n[other]\nenabled = false",
        "[verifier]\nenabled = true\n[other]\nenabled = true\nverify = false",
        "[verifier]\nenabled = true\n[testing]\nenabled = true",
    ] {
        assert_eq!(
            pool(toml).validate_with_plugins(None, &registry),
            Ok(()),
            "{}",
            toml
        );
    }
}

#[test]
fn a_verifier_needs_the_tenant_parameter() {
    let registry = verifiers();
    let pool = |toml: &str, tenant: Option<&str>| Pool {
        query_parser_enabled: true,
        plugins_require_parse: true,
        identity_tenant_parameter: tenant.map(Into::into),
        plugins: Some(plugins(toml)),
        ..Default::default()
    };
    for toml in [
        "[verifier]\nenabled = true",
        "[testing]\nenabled = true\nverify_identity = true",
    ] {
        assert_eq!(
            pool(toml, None).validate_with_plugins(None, &registry),
            Err(Error::BadConfig),
            "{}",
            toml
        );
        assert_eq!(
            pool(toml, Some("app.tenant")).validate_with_plugins(None, &registry),
            Ok(()),
            "{}",
            toml
        );
    }
    // No verifier, no tenant parameter needed.
    for toml in [
        "[verifier]\nenabled = false",
        "[other]\nenabled = true\nverify = false",
        "[testing]\nenabled = true",
    ] {
        assert_eq!(
            pool(toml, None).validate_with_plugins(None, &registry),
            Ok(()),
            "{}",
            toml
        );
    }

    // Nor does a verifier a pool inherits from the global section skip it.
    let global = loaded(&registry, "[verifier]\nenabled = true");
    let inheriting = |tenant: Option<&str>| Pool {
        query_parser_enabled: true,
        identity_tenant_parameter: tenant.map(Into::into),
        ..Default::default()
    };
    assert_eq!(
        inheriting(None).validate_with_plugins(Some(&global), &registry),
        Err(Error::BadConfig)
    );
    assert_eq!(
        inheriting(Some("app.tenant")).validate_with_plugins(Some(&global), &registry),
        Ok(())
    );
}

#[tokio::test]
async fn identities_are_refused_where_the_verifier_is_unclear() {
    let registry = Arc::new(verifiers());

    // Validation doesn't allow these; the router refuses them anyway.
    let two = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[verifier]\nenabled = true\n[other]\nenabled = true",
        )),
    );
    assert_eq!(
        two.verify_identity(&token("tenant:acme"))
            .await
            .unwrap_err(),
        "more than one plugin verifies identities on the pool"
    );

    // A section no plugin parsed might name a verifier.
    let unparsed = router(
        registry.clone(),
        Some(plugins("[verifier]\nenabled = false")),
    );
    let unregistered = router(
        Arc::new(PluginRegistry::default()),
        Some(loaded(&registry, "[verifier]\nenabled = false")),
    );
    for router in [&unparsed, &unregistered] {
        assert_eq!(
            router
                .verify_identity(&token("tenant:acme"))
                .await
                .unwrap_err(),
            NOT_LOADED
        );
    }

    // The test-only testing verifier takes priority over both.
    let testing = "[testing]\nenabled = true\nverify_identity = true\n";
    let unparsed = router(
        registry.clone(),
        Some(plugins(&format!("{}[verifier]\nenabled = true", testing))),
    );
    let two = router(
        registry.clone(),
        Some(loaded(
            &registry,
            &format!(
                "{}[verifier]\nenabled = true\n[other]\nenabled = true",
                testing
            ),
        )),
    );
    for router in [&unparsed, &two] {
        let verified = router.verify_identity(&token("tenant:acme")).await.unwrap();
        assert_eq!(verified.tenant.as_deref(), Some("acme"));
        // The testing verifier's identities carry no claims.
        assert!(verified.claims.is_none());
    }
}

/// Verifies `tenant:late` once `release` is notified, after notifying
/// `entered`.
struct Blocked {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ConfiguredPlugin for Blocked {
    type Config = RewriterConfig;

    fn enabled(&self, config: &RewriterConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        _config: &RewriterConfig,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginOutput::Allow.into())
    }

    fn verifies_identity(&self, _config: &RewriterConfig) -> bool {
        true
    }

    async fn verify_identity(
        &self,
        _config: &RewriterConfig,
        _token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(VerifiedIdentity {
            tenant: Some("late".into()),
            ..Default::default()
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_verifier_that_takes_too_long_refuses_the_token() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = PluginRegistry::default();
    registry
        .register_configured(
            "slow_verifier",
            Blocked {
                entered: entered.clone(),
                release: release.clone(),
            },
        )
        .unwrap();
    let registry = Arc::new(registry);
    let timeout = std::time::Duration::from_millis(100);
    let mut blocked = QueryRouter::with_plugins(registry.clone());
    blocked.update_pool_settings(&PoolSettings {
        query_parser_enabled: true,
        plugins_require_parse: true,
        plugin_hook_timeout: timeout,
        plugins: Some(loaded(&registry, "[slow_verifier]\nenabled = true")),
        ..PoolSettings::default()
    });

    // Never released: refused once the pool's hook timeout is up.
    let acme = token("tenant:acme");
    let started = tokio::time::Instant::now();
    // The outer bound fails the test, rather than hanging it, if the verifier
    // ran without one.
    let bound = 10 * timeout;
    let (refused, ()) = tokio::time::timeout(bound, async {
        tokio::join!(blocked.verify_identity(&acme), entered.notified())
    })
    .await
    .expect("the verifier ran without a timeout");
    assert_eq!(
        refused.unwrap_err(),
        crate::messages::with_sqlstate(
            crate::plugins::HOOK_TIMED_OUT,
            "a plugin hook took too long"
        )
    );
    assert!(started.elapsed() >= timeout && started.elapsed() < 2 * timeout);
    assert_eq!(count_of("slow_verifier", Hook::VerifyIdentity, "PH001"), 1);

    // Released in time, its answer is kept.
    let (verified, ()) = tokio::time::timeout(bound, async {
        tokio::join!(blocked.verify_identity(&acme), async {
            entered.notified().await;
            release.notify_one();
        })
    })
    .await
    .unwrap();
    assert_eq!(verified.unwrap().tenant.as_deref(), Some("late"));
    // Only errors count.
    assert_eq!(count_of("slow_verifier", Hook::VerifyIdentity, "PH001"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_verifier_that_gets_no_hook_slot_refuses_the_token() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = PluginRegistry::default();
    registry
        .register_configured(
            "blocked",
            Blocked {
                entered: entered.clone(),
                release: release.clone(),
            },
        )
        .unwrap();
    let registry = Arc::new(registry);
    let pool = |timeout| {
        let mut router = QueryRouter::with_plugins(registry.clone());
        router.update_pool_settings(&PoolSettings {
            query_parser_enabled: true,
            plugins_require_parse: true,
            plugin_hook_timeout: timeout,
            plugins: Some(loaded(&registry, "[blocked]\nenabled = true")),
            ..PoolSettings::default()
        });
        router
    };
    let quick = std::time::Duration::from_millis(100);
    // The slow pool's verifier holds the only slot past the quick pool's
    // timeout, so the quick pool's can't start.
    let slow = pool(100 * quick);
    let fast = pool(quick);
    let slots = tokio::sync::Semaphore::new(1);
    let acme = token("tenant:acme");
    let busy_before = count_of(NO_PLUGIN, Hook::VerifyIdentity, "PH002");

    let started = tokio::time::Instant::now();
    // The outer bound fails the test, rather than hanging it, if the quick
    // pool's verifier ran without a slot: nothing would release it.
    let (late, busy) = tokio::time::timeout(10 * quick, async {
        tokio::join!(
            slow.verify_identity_in(&slots, &acme, Verifying::Set),
            async {
                entered.notified().await;
                let busy = fast.verify_identity_in(&slots, &acme, Verifying::Set).await;
                release.notify_one();
                busy
            }
        )
    })
    .await
    .expect("a verifier ran without a hook slot");
    assert_eq!(
        busy.unwrap_err(),
        crate::messages::with_sqlstate(
            crate::plugins::HOOK_BUSY,
            "too many plugin hooks are running"
        )
    );
    assert!(started.elapsed() >= quick && started.elapsed() < 2 * quick);
    // No plugin ran: counted for none.
    assert_eq!(count_of("blocked", Hook::VerifyIdentity, "PH002"), 0);
    assert_eq!(
        count_of(NO_PLUGIN, Hook::VerifyIdentity, "PH002"),
        busy_before + 1
    );
    // The verifier that held the slot still answers.
    assert_eq!(late.unwrap().tenant.as_deref(), Some("late"));
    // And the slot is free again.
    let (verified, ()) = tokio::time::timeout(10 * quick, async {
        tokio::join!(
            fast.verify_identity_in(&slots, &acme, Verifying::Set),
            async {
                entered.notified().await;
                release.notify_one();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(verified.unwrap().tenant.as_deref(), Some("late"));

    // Verified again after a reload, a verifier with no slot is still the
    // hook's PH002, to the client and in the count, not the 0A000 a refusal
    // ends the session with.
    let busy_before = count_of(NO_PLUGIN, Hook::VerifyIdentity, "PH002");
    let (late, busy) = tokio::time::timeout(10 * quick, async {
        tokio::join!(
            slow.verify_identity_in(&slots, &acme, Verifying::Set),
            async {
                entered.notified().await;
                let busy = fast
                    .verify_identity_in(&slots, &acme, Verifying::Again)
                    .await;
                release.notify_one();
                busy
            }
        )
    })
    .await
    .expect("a verifier ran without a hook slot");
    assert_eq!(
        busy.unwrap_err(),
        crate::messages::with_sqlstate(
            crate::plugins::HOOK_BUSY,
            "too many plugin hooks are running"
        )
    );
    assert_eq!(
        count_of(NO_PLUGIN, Hook::VerifyIdentity, "PH002"),
        busy_before + 1
    );
    assert_eq!(count_of("blocked", Hook::VerifyIdentity, "0A000"), 0);
    assert_eq!(late.unwrap().tenant.as_deref(), Some("late"));
}

#[tokio::test]
async fn a_verifiers_refusals_and_failures_are_counted_for_it() {
    let mut registry = PluginRegistry::default();
    registry
        .register_configured("counted_verifier", Verifier)
        .unwrap();
    registry
        .register_configured("counted_other", Verifier)
        .unwrap();
    let registry = Arc::new(registry);
    let on = router(
        registry.clone(),
        Some(loaded(&registry, "[counted_verifier]\nenabled = true")),
    );
    on.verify_identity(&token("tenant:acme")).await.unwrap();
    // Refusals, whatever code or token they carry, are 28000 to the client.
    for refused in ["forged", "code:PH001", "echo:secret-91d2"] {
        on.verify_identity(&token(refused)).await.unwrap_err();
    }
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "28000"),
        3
    );
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "PH001"),
        0
    );

    // Verified again after a reload, a refusal ends the session: the
    // client gets 0A000, and that's what's counted.
    on.verify_identity_again(&token("forged"))
        .await
        .unwrap_err();
    // A plugin's own PH001 is a refusal like any other: stripped, it ends
    // the session, and isn't counted as a hook timeout.
    on.verify_identity_again(&token("code:PH001"))
        .await
        .unwrap_err();
    on.verify_identity_again(&token("tenant:acme"))
        .await
        .unwrap();
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "0A000"),
        2
    );
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "PH001"),
        0
    );
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "28000"),
        3
    );

    // Core's own refusals aren't counted: no plugin failed.
    let both = router(
        registry.clone(),
        Some(loaded(
            &registry,
            "[counted_verifier]\nenabled = true\n[counted_other]\nenabled = true",
        )),
    );
    both.verify_identity(&token("tenant:acme"))
        .await
        .unwrap_err();
    assert_eq!(
        count_of("counted_verifier", Hook::VerifyIdentity, "28000"),
        3
    );
    assert_eq!(count_of("counted_other", Hook::VerifyIdentity, "28000"), 0);
}

/// A configured plugin whose hook fails, which denies the statement.
struct Failing;

#[async_trait]
impl ConfiguredPlugin for Failing {
    type Config = SyntheticConfig;

    fn enabled(&self, config: &SyntheticConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        _config: &SyntheticConfig,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Err(Error::PluginError(crate::messages::with_sqlstate(
            "22023",
            "the plugin failed",
        )))
    }
}

#[tokio::test]
async fn a_configured_plugins_error_in_the_chain_is_counted_for_it() {
    let mut registry = PluginRegistry::default();
    registry
        .register_configured("failing_in_chain", Failing)
        .unwrap();
    let registry = Arc::new(registry);
    let on = router(
        registry.clone(),
        Some(loaded(&registry, "[failing_in_chain]\nenabled = true")),
    );
    let ast = parse("SELECT 1");
    let query = on.execute_query_plugins(&ast, None).await.unwrap();
    let parsed = on.execute_parse_plugins(&ast, &[], None).await.unwrap();
    for result in [query, parsed] {
        assert!(matches!(result.output, PluginOutput::Deny(_)));
    }
    // The client gets the Deny's 42501, not the plugin's code.
    assert_eq!(count_of("failing_in_chain", Hook::Query, "42501"), 1);
    assert_eq!(count_of("failing_in_chain", Hook::Parse, "42501"), 1);
    assert_eq!(count_of("failing_in_chain", Hook::Query, "22023"), 0);
}

/// Refuses another plugin's rewrite that names its table. With `rewrite`, it
/// rewrites `SELECT 1` itself into a SELECT of no columns from its table,
/// which prints as SQL sqlparser can't parse.
struct Guard;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GuardConfig {
    enabled: bool,
    table: String,
    #[serde(default)]
    rewrite: bool,
    #[serde(default)]
    as_built: bool,
}

#[async_trait]
impl ConfiguredPlugin for Guard {
    type Config = GuardConfig;

    fn enabled(&self, config: &GuardConfig) -> bool {
        config.enabled
    }

    async fn on_query(
        &self,
        config: &GuardConfig,
        _context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        use sqlparser::ast::{Expr, Ident, SetExpr};

        match statements {
            [statement] if config.rewrite && statement.to_string() == "SELECT 1" => {
                // A WHERE without a condition: it names the table, but its
                // SQL can't be parsed.
                let mut rewrite = parse(&format!("SELECT 1 FROM {}", config.table));
                if let Statement::Query(query) = &mut rewrite[0] {
                    if let SetExpr::Select(select) = query.body.as_mut() {
                        select.selection = Some(Expr::Identifier(Ident::new("")));
                    }
                }
                Ok(PluginOutput::Overwrite(rewrite).into())
            }
            _ => Ok(PluginOutput::Allow.into()),
        }
    }

    fn checks_rewrites(&self, _config: &GuardConfig) -> bool {
        true
    }

    fn check_rewrite(&self, config: &GuardConfig, sent: &[Statement]) -> Result<(), String> {
        use sqlparser::ast::visit_relations;
        use std::ops::ControlFlow;

        let named = sent.iter().any(|statement| {
            let found = visit_relations(statement, |relation| {
                match relation.to_string() == config.table {
                    true => ControlFlow::Break(()),
                    false => ControlFlow::Continue(()),
                }
            });
            found.is_break()
        });
        match named {
            true => Err(crate::messages::with_sqlstate(
                crate::plugins::PLUGIN_DENIED,
                "guard: a plugin's rewrite names its table",
            )),
            false => Ok(()),
        }
    }

    fn rewrite_checked_as_built(&self, config: &GuardConfig) -> bool {
        config.as_built
    }
}

/// The output of `SELECT 1` on a router of `registry` with `sections`, on the
/// simple and the extended protocol, which must agree.
async fn rewritten(
    registry: &Arc<PluginRegistry>,
    sections: &str,
    table_access: bool,
) -> PluginOutput {
    let mut plugins = loaded(registry, sections);
    if table_access {
        plugins.table_access = Some(TableAccess {
            enabled: true,
            tables: vec!["blocked".into()],
        });
    }
    let on = router(registry.clone(), Some(plugins));
    let ast = parse("SELECT 1");
    let query = on.execute_query_plugins(&ast, None).await.unwrap().output;
    let parsed = on
        .execute_parse_plugins(&ast, &[23], None)
        .await
        .unwrap()
        .output;
    assert_eq!(query, parsed, "{}", sections);
    query
}

#[tokio::test]
async fn a_plugin_that_checks_rewrites_refuses_another_plugins_but_not_its_own() {
    let denied = PluginOutput::Deny(crate::messages::with_sqlstate(
        crate::plugins::PLUGIN_DENIED,
        "guard: a plugin's rewrite names its table",
    ));
    let guarded = "[rewriter]\nenabled = true\n[guard]\ntable = \"guarded\"\nenabled = ";
    for (to, guard_on, expected) in [
        ("SELECT * FROM guarded", true, Some(&denied)),
        (
            "SELECT 1 FROM other JOIN guarded ON true",
            true,
            Some(&denied),
        ),
        ("SELECT * FROM other", true, None),
        // Off, it checks nothing.
        ("SELECT * FROM guarded", false, None),
    ] {
        let mut registry = registry_with_rewrite_to(to);
        registry.register_configured("guard", Guard).unwrap();
        let registry = Arc::new(registry);
        let sections = format!("{}{}", guarded, guard_on);
        match (rewritten(&registry, &sections, false).await, expected) {
            (output, Some(expected)) => assert_eq!(&output, expected, "{}", to),
            (output, None) => assert_eq!(output, PluginOutput::Overwrite(parse(to))),
        }
    }

    // Its own rewrite names its table, and isn't checked by it: nor parsed
    // again, since no other check applies.
    let mut registry = PluginRegistry::default();
    registry.register_configured("guard", Guard).unwrap();
    let registry = Arc::new(registry);
    let own = "[guard]\nenabled = true\ntable = \"guarded\"\nrewrite = true";
    match rewritten(&registry, own, false).await {
        PluginOutput::Overwrite(statements) => {
            assert_eq!(statements_to_sql(&statements), "SELECT 1 FROM guarded WHERE ")
        }
        output => panic!("{:?}", output),
    }
}

#[tokio::test]
async fn a_rewrite_checked_as_built_isnt_parsed_again() {
    let mut registry = PluginRegistry::default();
    registry.register_configured("guard", Guard).unwrap();
    let registry = Arc::new(registry);
    let own = "[guard]\nenabled = true\ntable = \"guarded\"\nrewrite = true\nas_built = ";

    // table_access checks the rewrite as the plugin built it.
    match rewritten(&registry, &format!("{}true", own), true).await {
        PluginOutput::Overwrite(statements) => {
            assert_eq!(statements_to_sql(&statements), "SELECT 1 FROM guarded WHERE ")
        }
        output => panic!("{:?}", output),
    }
    // Otherwise it's parsed again, and can't be.
    assert_eq!(
        rewritten(&registry, &format!("{}false", own), true).await,
        PluginOutput::Deny(crate::messages::with_sqlstate(
            PLUGIN_FAILED,
            "a plugin's rewrite can't be parsed",
        ))
    );
}

#[tokio::test]
async fn a_rewrite_is_parsed_again_only_if_a_check_applies() {
    let (mut registry, _) = registry();
    registry.register_configured("guard", Guard).unwrap();
    registry.register_configured("other_guard", Guard).unwrap();
    let registry = Arc::new(registry);
    let own = "[guard]\nenabled = true\ntable = \"guarded\"\nrewrite = true\n";
    let unparsed = PluginOutput::Deny(crate::messages::with_sqlstate(
        PLUGIN_FAILED,
        "a plugin's rewrite can't be parsed",
    ));
    for (others, table_access, parsed_again) in [
        ("", false, false),
        // A plugin that doesn't check rewrites.
        ("[synthetic]\nenabled = true\n", false, false),
        (
            "[other_guard]\nenabled = false\ntable = \"t\"\n",
            false,
            false,
        ),
        (
            "[other_guard]\nenabled = true\ntable = \"t\"\n",
            false,
            true,
        ),
        ("", true, true),
    ] {
        let sections = format!("{}{}", own, others);
        let output = rewritten(&registry, &sections, table_access).await;
        match parsed_again {
            true => assert_eq!(output, unparsed, "{}", sections),
            false => assert!(
                matches!(output, PluginOutput::Overwrite(_)),
                "{}: {:?}",
                sections,
                output
            ),
        }
    }
}
