use super::*;
use crate::{
    plugins::hook_errors::{count_of, Hook},
    plugins::{run_hook, PluginOutput, StatementHooks, StatementPlan},
    query_router::QueryRouter,
};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct CountingPlugin(Arc<AtomicUsize>, bool);

#[async_trait]
impl RegisteredPlugin for CountingPlugin {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(if self.1 {
            PluginOutput::Deny("synthetic refusal".into())
        } else {
            PluginOutput::Allow
        }
        .into())
    }

    async fn on_parse(
        &self,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        assert_eq!(context.declared_param_types, [23]);
        self.on_query(context, statements).await
    }
}

fn statement() -> Vec<Statement> {
    Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1").unwrap()
}

#[tokio::test]
async fn duplicate_or_empty_registration_is_rejected_without_replacing_first() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("first", CountingPlugin(calls.clone(), false))
        .unwrap();
    assert_eq!(
        registry.register("first", CountingPlugin(calls.clone(), true)),
        Err(Error::BadConfig)
    );
    assert_eq!(
        registry.register("", CountingPlugin(calls.clone(), false)),
        Err(Error::BadConfig)
    );
    assert_eq!(registry.plugins().count(), 1);
    let router = QueryRouter::with_plugins(Arc::new(registry));
    assert_eq!(
        router
            .execute_query_plugins(&statement(), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Allow
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dispatches_without_toml_plugins_and_cannot_disable_parser_to_bypass() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("external", CountingPlugin(calls.clone(), false))
        .unwrap();
    let mut router = QueryRouter::with_plugins(Arc::new(registry));
    let settings = crate::pool::PoolSettings {
        query_parser_enabled: false,
        plugins: None,
        ..crate::pool::PoolSettings::default()
    };
    router.update_pool_settings(&settings);
    assert!(router.plugins_enabled());
    assert!(router.plugins_require_parse());
    assert!(router.rejects_unparsed(&crate::messages::simple_query("not valid sql")));
    router
        .execute_query_plugins(&statement(), None)
        .await
        .unwrap();
    router
        .execute_parse_plugins(&statement(), &[23], None)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn deny_stops_ordered_chain_and_does_not_escape_into_another_router() {
    let denied = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("deny", CountingPlugin(denied.clone(), true))
        .unwrap();
    registry
        .register("skipped", CountingPlugin(skipped.clone(), false))
        .unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    assert_eq!(
        router
            .execute_query_plugins(&statement(), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Deny("synthetic refusal".into())
    );
    assert_eq!(denied.load(Ordering::SeqCst), 1);
    assert_eq!(skipped.load(Ordering::SeqCst), 0);
    let empty = QueryRouter::with_plugins(Arc::new(PluginRegistry::default()));
    assert_eq!(
        empty
            .execute_query_plugins(&statement(), None)
            .await
            .unwrap()
            .output,
        PluginOutput::Allow
    );
}

struct PlanPlugin;
#[async_trait]
impl StatementHooks for PlanPlugin {}
#[async_trait]
impl RegisteredPlugin for PlanPlugin {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginResult {
            output: PluginOutput::Allow,
            plan: Some(StatementPlan::new(Arc::new(PlanPlugin), Box::new(()))),
        })
    }
}

#[tokio::test]
async fn rejects_multiple_plans_instead_of_silently_dropping_one() {
    let mut registry = PluginRegistry::default();
    registry.register("one", PlanPlugin).unwrap();
    registry.register("two", PlanPlugin).unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    let result = router
        .execute_query_plugins(&statement(), None)
        .await
        .unwrap();
    assert!(matches!(result.output, PluginOutput::Deny(_)));
    assert!(result.plan.is_none());
}

struct RewritePlugin;
#[async_trait]
impl RegisteredPlugin for RewritePlugin {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Ok(PluginOutput::Overwrite(statements.to_vec()).into())
    }
}

#[tokio::test]
async fn rejects_a_rewrite_and_plan_from_different_plugins_in_either_order() {
    for plan_first in [false, true] {
        let mut registry = PluginRegistry::default();
        if plan_first {
            registry.register("plan", PlanPlugin).unwrap();
            registry.register("rewrite", RewritePlugin).unwrap();
        } else {
            registry.register("rewrite", RewritePlugin).unwrap();
            registry.register("plan", PlanPlugin).unwrap();
        }
        let router = QueryRouter::with_plugins(Arc::new(registry));
        for result in [
            router
                .execute_query_plugins(&statement(), None)
                .await
                .unwrap(),
            router
                .execute_parse_plugins(&statement(), &[], None)
                .await
                .unwrap(),
        ] {
            assert!(matches!(result.output, PluginOutput::Deny(_)));
            assert!(result.plan.is_none());
        }
    }
}

struct RewriteAndPlanPlugin;
#[async_trait]
impl RegisteredPlugin for RewriteAndPlanPlugin {
    async fn on_query(
        &self,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        let mut result = PlanPlugin.on_query(context, statements).await?;
        result.output = PluginOutput::Overwrite(statements.to_vec());
        Ok(result)
    }
}

#[tokio::test]
async fn one_plugin_can_supply_a_rewrite_and_its_matching_plan() {
    let mut registry = PluginRegistry::default();
    registry.register("combined", RewriteAndPlanPlugin).unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    for result in [
        router
            .execute_query_plugins(&statement(), None)
            .await
            .unwrap(),
        router
            .execute_parse_plugins(&statement(), &[], None)
            .await
            .unwrap(),
    ] {
        assert_eq!(result.output, PluginOutput::Overwrite(statement()));
        assert!(result.plan.is_some());
    }
}

struct NeverCompletes;
#[async_trait]
impl RegisteredPlugin for NeverCompletes {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn registered_hook_obeys_existing_timeout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("before_pending", CountingPlugin(calls.clone(), false))
        .unwrap();
    registry.register("pending", NeverCompletes).unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    let ast = statement();
    let result = run_hook(
        Duration::from_millis(10),
        router.execute_query_plugins(&ast, None),
    )
    .await;
    let err = result.unwrap_err();
    assert_eq!(
        err,
        Error::PluginError(crate::messages::with_sqlstate(
            crate::plugins::HOOK_TIMED_OUT,
            "a plugin hook took too long"
        ))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Counted for the plugin running at the deadline, not the one before it.
    assert_eq!(
        router.chain_failed(Hook::Query, &err),
        crate::messages::with_sqlstate(
            crate::plugins::HOOK_TIMED_OUT,
            "a plugin hook took too long"
        )
    );
    assert_eq!(count_of("pending", Hook::Query, "PH001"), 1);
    assert_eq!(count_of("before_pending", Hook::Query, "PH001"), 0);
}

struct Failing;
#[async_trait]
impl RegisteredPlugin for Failing {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        _statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        Err(Error::PluginError("the plugin failed".into()))
    }
}

#[tokio::test]
async fn a_registered_plugins_error_is_counted_for_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("before_failing", CountingPlugin(calls.clone(), false))
        .unwrap();
    registry.register("failing", Failing).unwrap();
    registry
        .register("after_failing", CountingPlugin(calls.clone(), false))
        .unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    let err = router
        .execute_parse_plugins(&statement(), &[23], None)
        .await
        .unwrap_err();
    // With no code of its own, the client gets XX000.
    assert_eq!(
        router.chain_failed(Hook::Parse, &err),
        crate::messages::with_sqlstate(crate::plugins::PLUGIN_FAILED, "the plugin failed")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(count_of("failing", Hook::Parse, "XX000"), 1);
    assert_eq!(count_of("before_failing", Hook::Parse, "XX000"), 0);
}

#[tokio::test]
async fn a_plans_hook_errors_are_counted_for_the_plugin_that_planned_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::default();
    registry
        .register("before_planner", CountingPlugin(calls, false))
        .unwrap();
    registry.register("planner", PlanPlugin).unwrap();
    let router = QueryRouter::with_plugins(Arc::new(registry));
    let plan = router
        .execute_query_plugins(&statement(), None)
        .await
        .unwrap()
        .plan
        .unwrap();
    assert_eq!(plan.plugin, "planner");

    let refused = Error::PluginError(crate::messages::with_sqlstate("22023", "a bad value"));
    assert_eq!(
        plan.hook_failed(Hook::Bind, &refused),
        crate::messages::with_sqlstate("22023", "a bad value")
    );
    plan.hook_failed(Hook::Rows, &Error::PluginError("no code".into()));
    assert_eq!(count_of("planner", Hook::Bind, "22023"), 1);
    assert_eq!(count_of("planner", Hook::Rows, "XX000"), 1);
    assert_eq!(count_of("before_planner", Hook::Bind, "22023"), 0);

    // A plan no plugin chain stamped is counted for none.
    let unstamped = StatementPlan::new(Arc::new(PlanPlugin), Box::new(()));
    assert_eq!(unstamped.plugin, crate::plugins::hook_errors::NO_PLUGIN);
}
