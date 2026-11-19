//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.
//!
//! The check goes by name: a statement is refused if it names a forbidden
//! table as a relation it reads or writes, or as the table of DDL, a
//! privilege, a comment, a trigger, a foreign key, `LIKE`, `SELECT INTO`,
//! `TABLE t` or a rename. It doesn't follow objects that already exist on the
//! server. A view, a parent table or partition tree, a function, a rule or a
//! trigger that reaches a forbidden table is used by its own name, so a
//! statement naming only it is allowed. Neither does it read names inside
//! strings: a foreign table's `OPTIONS (table_name '...')`, `dblink` or
//! `table_to_xml`/`query_to_xml`, nor a function body given as a string, which
//! is refused instead while pgcat requires parsing (`plugins_require_parse`, or
//! a process-wide plugin) and forwarded otherwise. Block those on the server,
//! with privileges, if they matter.

use async_trait::async_trait;
use sqlparser::ast::{
    visit_relations, visit_statements, AlterIndexOperation, AlterTableOperation,
    AlterViewOperation, ColumnDef, ColumnOption, CommentObject, CopySource, CreateFunction,
    CreateFunctionBody, CreateProcedureBody, CreateTableLikeKind, DataType, Expr, GrantObjects,
    Ident, ObjectName, ObjectType, Query, RenameTableNameKind, SetExpr, Statement, TableConstraint,
    VacuumStatement, Visit, Visitor,
};
use std::borrow::Cow;

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

use log::debug;

use core::ops::ControlFlow;

pub struct TableAccess<'a> {
    pub enabled: bool,
    pub tables: &'a Vec<String>,
}

#[async_trait]
impl<'a> Plugin for TableAccess<'a> {
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        let mut found = None;
        let _ = visit_relations(ast, |relation| {
            found = self.forbidden(relation);
            match found {
                Some(_) => ControlFlow::<()>::Break(()),
                None => ControlFlow::<()>::Continue(()),
            }
        });

        // `visit_relations` doesn't visit the names in `unvisited_names`, in
        // any statement, nested ones too.
        let found = found.or_else(|| {
            let mut found = None;
            let _ = visit_statements(ast, |statement| {
                found = unvisited_names(statement)
                    .iter()
                    .find_map(|name| self.forbidden_name(name));
                match found {
                    Some(_) => ControlFlow::<()>::Break(()),
                    None => ControlFlow::<()>::Continue(()),
                }
            });
            found
        });

        // Nor the tables `SELECT ... INTO` creates or `TABLE t` reads, in any
        // query.
        let found = found.or_else(|| {
            let mut names = QueryNames(Vec::new());
            let _ = ast.visit(&mut names);
            names.0.iter().find_map(|name| self.forbidden_name(name))
        });

        // A VACUUM without a table processes every table, and a GRANT or
        // REVOKE on all tables in a schema every table there: forbidden ones
        // too.
        if found.is_none() && !self.tables.is_empty() {
            if let Some(every) = every_table(ast) {
                debug!("Blocking {}", every);

                return Ok(PluginOutput::Deny(format!(
                    "permission for {} denied",
                    every
                )));
            }
        }

        // A function body given as a string isn't parsed, so the tables it
        // names can't be checked. One given as SQL (`BEGIN ATOMIC`, `RETURN`)
        // is, above. The refusal comes with plugins_require_parse, which also
        // refuses the bodies the parser can't read (`DO`, a function with an
        // option it doesn't know). Without it, table access checks names only.
        if found.is_none()
            && !self.tables.is_empty()
            && query_router.plugins_require_parse()
            && has_string_body(ast)
        {
            debug!("Blocking a function body given as a string");

            return Ok(PluginOutput::Deny(
                "permission for a function body given as a string denied".into(),
            ));
        }

        if let Some(found) = found {
            debug!("Blocking access to table \"{}\"", found);

            Ok(PluginOutput::Deny(format!(
                "permission for table \"{}\" denied",
                found
            )))
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}

/// The last part of each name a statement has for a table that
/// `visit_relations` doesn't visit. The match lists the statements that have
/// one, by field:
///
/// - `COPY t` and `VACUUM t`;
/// - the objects a `DROP`, `GRANT`, `REVOKE` or `COMMENT ON` names, for kinds
///   that share the namespace of tables (a column's table for
///   `COMMENT ON COLUMN`);
/// - the table of `CREATE TRIGGER` and `DROP TRIGGER`, and the one a
///   constraint trigger's `FROM` references;
/// - the parents a `CREATE TABLE` or `CREATE FOREIGN TABLE` inherits from, the
///   tables it copies with `LIKE`, and the tables its foreign keys reference;
/// - the table an `ALTER TABLE` attaches or detaches as a partition, the
///   parent it adds or removes with `INHERIT` or `NO INHERIT`, the tables the
///   foreign keys it adds reference, and the new name it renames the table to;
/// - the new name of `ALTER VIEW`, both names of `ALTER INDEX ... RENAME TO`,
///   and both names of `RENAME TABLE`.
fn unvisited_names(statement: &Statement) -> Vec<Cow<'_, Ident>> {
    let names: Vec<&ObjectName> = match statement {
        Statement::Copy {
            source: CopySource::Table { table_name, .. },
            ..
        } => vec![table_name],
        Statement::Vacuum(vacuum) => vacuum_target(vacuum).into_iter().collect(),
        Statement::Drop {
            object_type:
                ObjectType::Table
                | ObjectType::ForeignTable
                | ObjectType::View
                | ObjectType::MaterializedView
                | ObjectType::Index
                | ObjectType::Sequence,
            names,
            table,
            ..
        } => names.iter().chain(table).collect(),
        Statement::Grant(grant) => granted(grant.objects.as_ref()),
        Statement::Revoke(revoke) => granted(revoke.objects.as_ref()),
        Statement::Comment {
            object_type,
            object_name,
            ..
        } => {
            return match object_type {
                CommentObject::Table
                | CommentObject::ForeignTable
                | CommentObject::View
                | CommentObject::MaterializedView
                | CommentObject::Index
                | CommentObject::Sequence => last_part(object_name)
                    .map(Cow::Borrowed)
                    .into_iter()
                    .collect(),
                // `table.column`, or `schema.table.column`.
                CommentObject::Column => object_name
                    .0
                    .iter()
                    .rev()
                    .nth(1)
                    .and_then(|part| part.as_ident())
                    .map(Cow::Borrowed)
                    .into_iter()
                    .collect(),
                _ => Vec::new(),
            };
        }
        Statement::CreateTrigger(trigger) => std::iter::once(&trigger.table_name)
            .chain(&trigger.referenced_table_name)
            .collect(),
        Statement::DropTrigger(trigger) => trigger.table_name.iter().collect(),
        Statement::CreateTable(table) => {
            let like = match &table.like {
                Some(
                    CreateTableLikeKind::Parenthesized(like) | CreateTableLikeKind::Plain(like),
                ) => Some(&like.name),
                None => None,
            };
            let names = table
                .inherits
                .iter()
                .flatten()
                .chain(like)
                .chain(
                    table
                        .columns
                        .iter()
                        .flat_map(|column| &column.options)
                        .filter_map(|option| references(&option.option)),
                )
                .chain(table.constraints.iter().filter_map(referenced))
                .filter_map(last_part)
                .map(Cow::Borrowed);
            return names
                .chain(table.columns.iter().filter_map(misparsed_like))
                .collect();
        }
        Statement::CreateForeignTable(table) => {
            let names = table.inherits.iter().flatten().filter_map(last_part);
            return names
                .map(Cow::Borrowed)
                .chain(table.columns.iter().filter_map(misparsed_like))
                .collect();
        }
        Statement::AlterTable(alter) => alter
            .operations
            .iter()
            .flat_map(|operation| -> Vec<&ObjectName> {
                match operation {
                    AlterTableOperation::AttachPartitionOf { partition, .. }
                    | AlterTableOperation::DetachPartitionOf { partition, .. } => vec![partition],
                    AlterTableOperation::Inherit { parent }
                    | AlterTableOperation::NoInherit { parent } => vec![parent],
                    AlterTableOperation::AddConstraint { constraint, .. } => {
                        referenced(constraint).into_iter().collect()
                    }
                    AlterTableOperation::AddColumn { column_def, .. } => column_def
                        .options
                        .iter()
                        .filter_map(|option| references(&option.option))
                        .collect(),
                    AlterTableOperation::RenameTable {
                        table_name: RenameTableNameKind::As(name) | RenameTableNameKind::To(name),
                    } => vec![name],
                    _ => Vec::new(),
                }
            })
            .collect(),
        Statement::AlterView(alter) => match &alter.operation {
            AlterViewOperation::RenameTo { new_name } => vec![new_name],
            _ => Vec::new(),
        },
        // Postgres runs `ALTER INDEX t RENAME TO n` on a table too.
        Statement::AlterIndex {
            name,
            operation: AlterIndexOperation::RenameIndex { index_name },
        } => vec![name, index_name],
        Statement::RenameTable(renames) => renames
            .iter()
            .flat_map(|rename| [&rename.old_name, &rename.new_name])
            .collect(),
        _ => Vec::new(),
    };
    names
        .into_iter()
        .filter_map(last_part)
        .map(Cow::Borrowed)
        .collect()
}

/// The objects a GRANT or REVOKE names, of kinds that share the namespace of
/// tables.
fn granted(objects: Option<&GrantObjects>) -> Vec<&ObjectName> {
    match objects {
        Some(
            GrantObjects::Tables(names)
            | GrantObjects::Views(names)
            | GrantObjects::Sequences(names),
        ) => names.iter().collect(),
        _ => Vec::new(),
    }
}

/// The table a `LIKE` the parser reads as a column copies. It reads
/// `CREATE TABLE c (a int, LIKE t)`, and any `LIKE` in `CREATE FOREIGN TABLE`,
/// as a column named `like` of type `t`. Postgres reads an unquoted `like`
/// there as `LIKE`, never as a column name. A `t` the parser knows as a type
/// name (`json`, `text`, `timestamptz`) becomes that type, which prints as the
/// word it read.
fn misparsed_like(column: &ColumnDef) -> Option<Cow<'_, Ident>> {
    if column.name.quote_style.is_some() || !column.name.value.eq_ignore_ascii_case("like") {
        return None;
    }
    match &column.data_type {
        DataType::Custom(name, _) => last_part(name).map(Cow::Borrowed),
        other => Some(Cow::Owned(Ident::new(other.to_string()))),
    }
}

/// The table a column's `REFERENCES` names.
fn references(option: &ColumnOption) -> Option<&ObjectName> {
    match option {
        ColumnOption::ForeignKey(key) => Some(&key.foreign_table),
        _ => None,
    }
}

/// The table a table constraint's `FOREIGN KEY ... REFERENCES` names.
fn referenced(constraint: &TableConstraint) -> Option<&ObjectName> {
    match constraint {
        TableConstraint::ForeignKey(key) => Some(&key.foreign_table),
        _ => None,
    }
}

fn last_part(name: &ObjectName) -> Option<&Ident> {
    name.0.last()?.as_ident()
}

/// The tables `SELECT ... INTO` creates and `TABLE t` reads, in every query:
/// neither is an `ObjectName`, so `visit_relations` doesn't visit them.
struct QueryNames(Vec<Ident>);

impl QueryNames {
    fn body(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Select(select) => {
                let targets = select.into.iter().flat_map(|into| &into.targets);
                self.0.extend(targets.filter_map(|target| match target {
                    Expr::Identifier(ident) => Some(ident.clone()),
                    Expr::CompoundIdentifier(parts) => parts.last().cloned(),
                    _ => None,
                }));
            }
            SetExpr::Table(table) => self.0.extend(table.table_name.clone()),
            SetExpr::SetOperation { left, right, .. } => {
                self.body(left);
                self.body(right);
            }
            // A nested query is visited on its own.
            _ => (),
        }
    }
}

impl Visitor for QueryNames {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        self.body(&query.body);
        ControlFlow::Continue(())
    }
}

/// The statement that processes every table, in any statement: a VACUUM
/// without a table, or a GRANT or REVOKE on all tables in a schema.
fn every_table(ast: &Vec<Statement>) -> Option<&'static str> {
    let mut every = None;
    let _ = visit_statements(ast, |statement| {
        every = match statement {
            Statement::Vacuum(vacuum) if vacuum_target(vacuum).is_none() => {
                Some("VACUUM of every table")
            }
            Statement::Grant(grant)
                if matches!(grant.objects, Some(GrantObjects::AllTablesInSchema { .. })) =>
            {
                Some("GRANT on every table in a schema")
            }
            Statement::Revoke(revoke)
                if matches!(revoke.objects, Some(GrantObjects::AllTablesInSchema { .. })) =>
            {
                Some("REVOKE on every table in a schema")
            }
            _ => None,
        };
        match every {
            Some(_) => ControlFlow::<()>::Break(()),
            None => ControlFlow::<()>::Continue(()),
        }
    });
    every
}

/// Whether a statement, or one nested in it, creates a function or procedure
/// whose body isn't parsed SQL. A `RETURN` or `BEGIN ATOMIC` body is parsed,
/// and its tables are checked like any statement's. In Postgres every other
/// body is a string, `AS 'body'` or `AS $$body$$`, in any language, an object
/// file's too. Any other form the parser gives, or none, counts as one, so a
/// form it learns later isn't allowed unread.
fn has_string_body(ast: &Vec<Statement>) -> bool {
    visit_statements(ast, |statement| match statement {
        Statement::CreateFunction(CreateFunction {
            function_body: Some(CreateFunctionBody::Return(_) | CreateFunctionBody::BeginAtomic(_)),
            ..
        })
        | Statement::CreateProcedure {
            body: CreateProcedureBody::BeginAtomic(_),
            ..
        } => ControlFlow::Continue(()),
        Statement::CreateFunction(_) | Statement::CreateProcedure { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// The table a VACUUM processes, or `None` when it processes every table.
fn vacuum_target(vacuum: &VacuumStatement) -> Option<&ObjectName> {
    let name = vacuum.table_name.as_ref()?;
    // The parser reads an option after VACUUM, as in `VACUUM VERBOSE`, as a
    // table name. Postgres reads these words there as options, never as an
    // unquoted table name.
    if let [part] = name.0.as_slice() {
        if let Some(ident) = part.as_ident() {
            let option = ["analyse", "analyze", "freeze", "verbose"]
                .contains(&ident.value.to_ascii_lowercase().as_str());
            if ident.quote_style.is_none() && option {
                return None;
            }
        }
    }
    Some(name)
}

impl TableAccess<'_> {
    /// The table's name, if it is one of the forbidden tables.
    fn forbidden(&self, relation: &ObjectName) -> Option<String> {
        self.forbidden_name(last_part(relation)?)
    }

    /// The name, if it's one of the forbidden tables'.
    fn forbidden_name(&self, table: &Ident) -> Option<String> {
        // Postgres folds a name to lower case unless it's quoted.
        let name = match table.quote_style {
            Some(_) => table.value.clone(),
            None => table.value.to_ascii_lowercase(),
        };
        self.tables.contains(&name).then_some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::ConditionalStatements;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    const SECRETS: &[&str] = &["secrets"];

    /// Whether table access refuses `sql` with only `secrets` listed.
    async fn refused(sql: &str) -> bool {
        refused_by(&QueryRouter::new(), SECRETS, sql).await
    }

    async fn refused_by(router: &QueryRouter, tables: &[&str], sql: &str) -> bool {
        let tables = tables.iter().map(|table| table.to_string()).collect();
        let mut plugin = TableAccess {
            enabled: true,
            tables: &tables,
        };
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        matches!(
            plugin.run(router, &ast).await.unwrap(),
            PluginOutput::Deny(_)
        )
    }

    /// A router for a pool that sets plugins_require_parse.
    fn strict() -> QueryRouter {
        let mut router = QueryRouter::new();
        router.update_pool_settings(&crate::pool::PoolSettings {
            query_parser_enabled: true,
            plugins_require_parse: true,
            plugins: Some(crate::config::Plugins::default()),
            ..Default::default()
        });
        assert!(router.plugins_require_parse());
        router
    }

    #[tokio::test]
    async fn test_copy_of_a_forbidden_table_is_refused() {
        for sql in [
            "SELECT * FROM secrets",
            "COPY secrets TO STDOUT",
            "COPY public.secrets (a) TO STDOUT",
            "COPY (SELECT a FROM secrets) TO STDOUT",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }
        assert!(!refused("COPY other TO STDOUT").await);
    }

    #[tokio::test]
    async fn test_string_function_bodies_are_refused_with_plugins_require_parse() {
        let strict = strict();
        let string_bodies = [
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM secrets $$",
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
            "CREATE OR REPLACE FUNCTION f() RETURNS int AS $body$ SELECT 1 $body$ LANGUAGE sql",
            "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$",
            "CREATE FUNCTION f() RETURNS int AS 'MODULE_PATHNAME', 'f' LANGUAGE c",
            "SELECT 1; CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        ];
        for sql in string_bodies {
            assert!(refused_by(&strict, SECRETS, sql).await, "{}", sql);
            // Without plugins_require_parse, table access checks names only.
            assert!(!refused(sql).await, "{}", sql);
        }

        // A body given as SQL is checked by the tables it names, either way.
        for sql in [
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
             RETURN (SELECT count(*) FROM secrets)",
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC \
             SELECT count(*) FROM secrets; END",
        ] {
            assert!(refused_by(&strict, SECRETS, sql).await, "{}", sql);
            assert!(refused(sql).await, "{}", sql);
        }

        for sql in [
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
             RETURN (SELECT count(*) FROM other)",
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC \
             SELECT count(*) FROM other; END",
            "CREATE PROCEDURE p() BEGIN ATOMIC SELECT 1; END",
        ] {
            assert!(!refused_by(&strict, SECRETS, sql).await, "{}", sql);
        }

        // Nothing to protect, nothing refused.
        let sql = "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'";
        assert!(!refused_by(&strict, &[], sql).await);
    }

    #[tokio::test]
    async fn test_only_sql_function_bodies_are_read() {
        // sqlparser doesn't give these for Postgres today. A body that isn't
        // RETURN or BEGIN ATOMIC, or no body, is refused as unread.
        let tables = vec!["secrets".to_string()];
        let mut plugin = TableAccess {
            enabled: true,
            tables: &tables,
        };
        let strict = strict();
        let refusal =
            PluginOutput::Deny("permission for a function body given as a string denied".into());

        let sql = "CREATE PROCEDURE p() BEGIN ATOMIC SELECT 1; END";
        let mut ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        assert_eq!(
            plugin.run(&strict, &ast).await.unwrap(),
            PluginOutput::Allow
        );
        let Statement::CreateProcedure { body, .. } = &mut ast[0] else {
            panic!("{:?}", ast);
        };
        *body = CreateProcedureBody::As(ConditionalStatements::Sequence {
            statements: Vec::new(),
        });
        assert_eq!(plugin.run(&strict, &ast).await.unwrap(), refusal);

        let sql = "CREATE FUNCTION f() RETURNS int LANGUAGE sql RETURN 1";
        let mut ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        assert_eq!(
            plugin.run(&strict, &ast).await.unwrap(),
            PluginOutput::Allow
        );
        let Statement::CreateFunction(function) = &mut ast[0] else {
            panic!("{:?}", ast);
        };
        function.function_body = None;
        assert_eq!(plugin.run(&strict, &ast).await.unwrap(), refusal);
    }

    #[tokio::test]
    async fn test_vacuum_of_a_forbidden_table_is_refused() {
        for sql in [
            "VACUUM secrets",
            "VACUUM FULL secrets",
            "VACUUM FULL public.secrets",
            r#"VACUUM "secrets""#,
            r#"VACUUM FULL "public"."secrets""#,
            "VACUUM SECRETS",
            "SELECT 1; VACUUM secrets",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }
        for sql in [
            "VACUUM other",
            "VACUUM FULL public.other",
            r#"VACUUM "SECRETS""#,
        ] {
            assert!(!refused(sql).await, "{}", sql);
        }
    }

    #[tokio::test]
    async fn test_vacuum_of_every_table_is_refused() {
        // Without a table, VACUUM processes every table, so it's refused
        // while any table is forbidden. The parser reads the options in
        // `VACUUM VERBOSE` as a table name.
        for sql in [
            "VACUUM",
            "VACUUM FULL",
            "VACUUM;",
            "VACUUM VERBOSE",
            "VACUUM FULL FREEZE",
            "VACUUM analyze",
            "SELECT 1; VACUUM",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }

        // A table named like an option is one when it's quoted.
        assert!(!refused(r#"VACUUM "verbose""#).await);

        assert!(!refused_by(&QueryRouter::new(), &[], "VACUUM FULL").await);
    }

    #[tokio::test]
    async fn test_names_visit_relations_misses_are_checked() {
        for sql in [
            "DROP TABLE secrets",
            "DROP TABLE other, public.secrets",
            "DROP VIEW secrets",
            "DROP MATERIALIZED VIEW secrets",
            "DROP FOREIGN TABLE secrets",
            "GRANT SELECT ON secrets TO r",
            "GRANT SELECT ON TABLE public.secrets TO r",
            "REVOKE ALL ON secrets FROM r",
            "COMMENT ON TABLE secrets IS 'x'",
            "COMMENT ON COLUMN secrets.secret IS 'x'",
            "COMMENT ON COLUMN public.secrets.secret IS 'x'",
            "CREATE TRIGGER t AFTER INSERT ON secrets FOR EACH ROW EXECUTE FUNCTION f()",
            "CREATE CONSTRAINT TRIGGER t AFTER INSERT ON other FROM secrets \
             FOR EACH ROW EXECUTE FUNCTION f()",
            "DROP TRIGGER t ON public.secrets",
            "CREATE TABLE c (LIKE secrets)",
            "CREATE TABLE c LIKE secrets",
            "CREATE TABLE c (id int REFERENCES secrets(id))",
            "CREATE TABLE c (id int CONSTRAINT k REFERENCES secrets)",
            "CREATE TABLE c (id int, FOREIGN KEY (id) REFERENCES secrets(id))",
            "ALTER TABLE c ADD CONSTRAINT k FOREIGN KEY (id) REFERENCES secrets(id)",
            "ALTER TABLE c ADD COLUMN x int REFERENCES secrets(id)",
            "SELECT 1 INTO secrets",
            "SELECT * INTO public.secrets FROM other",
            "WITH q AS (SELECT 1) SELECT * INTO secrets FROM q",
            "SELECT 1 INTO secrets UNION ALL SELECT 2",
            "ALTER TABLE other RENAME TO secrets",
            "ALTER TABLE IF EXISTS other RENAME TO public.secrets",
            "ALTER VIEW other RENAME TO secrets",
            "ALTER MATERIALIZED VIEW other RENAME TO secrets",
            "ALTER INDEX other RENAME TO secrets",
            // Postgres renames a table with ALTER INDEX too.
            "ALTER INDEX secrets RENAME TO other",
            "ALTER INDEX public.secrets RENAME TO other",
            // A LIKE after a column.
            "CREATE TABLE c (a int, LIKE secrets)",
            "CREATE TABLE c (a int, like public.secrets)",
            // The parser reads every LIKE in CREATE FOREIGN TABLE as a column.
            "CREATE FOREIGN TABLE c (LIKE secrets) SERVER s",
            "CREATE FOREIGN TABLE c (a int, LIKE public.secrets) SERVER s",
            // Nested statements.
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC \
             COPY secrets TO STDOUT; END",
            "CREATE PROCEDURE p() BEGIN ATOMIC VACUUM secrets; END",
            "CREATE RULE r AS ON INSERT TO other DO ALSO COPY secrets TO STDOUT",
            "CREATE PROCEDURE p() BEGIN ATOMIC DROP TABLE secrets; END",
            // These reach every table in the schema, in any schema: the
            // forbidden names have none. A REVOKE is refused like a GRANT.
            "GRANT SELECT ON ALL TABLES IN SCHEMA public TO r",
            "GRANT SELECT ON ALL TABLES IN SCHEMA other_schema TO r",
            "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM r",
            "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC",
            "REVOKE SELECT ON secrets FROM r",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }

        // Other names, and the same name for objects outside the namespace of
        // tables, a column or a role, aren't.
        for sql in [
            "DROP TABLE other",
            "DROP ROLE secrets",
            "GRANT SELECT ON other TO secrets",
            "COMMENT ON SCHEMA secrets IS 'x'",
            "COMMENT ON COLUMN other.secrets IS 'x'",
            "CREATE TABLE c (LIKE other)",
            "CREATE TABLE c (id int REFERENCES other(id))",
            "SELECT 1 INTO other",
            "ALTER TABLE other RENAME TO another",
            "ALTER INDEX other RENAME TO another",
            "CREATE TABLE c (a int, LIKE other)",
            r#"CREATE TABLE c (a int, "like" secrets)"#,
            "CREATE FOREIGN TABLE c (LIKE other) SERVER s",
            r#"CREATE FOREIGN TABLE c (a int, "like" secrets) SERVER s"#,
            // Not the table, though they share its name.
            "CREATE INDEX secrets ON other (a)",
            "CREATE SEQUENCE secrets",
            "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO r",
        ] {
            assert!(!refused(sql).await, "{}", sql);
        }
    }

    #[tokio::test]
    async fn test_like_of_a_table_named_as_a_type_is_refused() {
        // The parser reads a LIKE after a column as a column named `like`, and
        // a table named as a type it knows (`json`) as that type.
        let tables = ["json", "string", "timestamptz"];
        for sql in [
            "CREATE TABLE c (a int, LIKE json)",
            "CREATE TABLE c (a int, LIKE string)",
            "CREATE TABLE c (a int, LIKE timestamptz)",
            "CREATE TABLE c (a int, like JSON)",
            "CREATE FOREIGN TABLE c (LIKE json) SERVER s",
            "CREATE FOREIGN TABLE c (a int, LIKE timestamptz) SERVER s",
        ] {
            assert!(
                refused_by(&QueryRouter::new(), &tables, sql).await,
                "{}",
                sql
            );
        }
        // A quoted "like" is a column, and a column of a type isn't a table.
        for sql in [
            r#"CREATE TABLE c (a int, "like" json)"#,
            "CREATE TABLE c (a json, b string, c timestamptz)",
            "CREATE TABLE c (a int, LIKE text)",
        ] {
            assert!(
                !refused_by(&QueryRouter::new(), &tables, sql).await,
                "{}",
                sql
            );
        }

        // Each type name the parser knows prints as the word it read, so a
        // table with that name is refused.
        let mut types = 0;
        for word in sqlparser::keywords::ALL_KEYWORDS {
            let sql = format!("CREATE TABLE c (a int, LIKE {})", word.to_ascii_lowercase());
            let Ok(ast) = Parser::parse_sql(&PostgreSqlDialect {}, &sql) else {
                continue;
            };
            if let Statement::CreateTable(table) = &ast[0] {
                if !matches!(table.columns[1].data_type, DataType::Custom(..)) {
                    types += 1;
                }
            }
            let table = word.to_ascii_lowercase();
            assert!(
                refused_by(&QueryRouter::new(), &[table.as_str()], &sql).await,
                "{}",
                sql
            );
        }
        assert!(types > 50, "{}", types);
    }

    #[tokio::test]
    async fn test_table_command_is_checked() {
        // `TABLE t` reads t. Nested queries are visited on their own.
        for sql in [
            "WITH x AS (TABLE secrets) SELECT * FROM x",
            "CREATE TABLE c AS TABLE secrets",
            "SELECT * FROM other WHERE id IN (TABLE secrets)",
            "SELECT * FROM other WHERE id IN (TABLE public.secrets)",
            r#"WITH x AS (TABLE "secrets") SELECT * FROM x"#,
            "SELECT * FROM (TABLE secrets) s",
            "SELECT 1 UNION TABLE secrets",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }
        for sql in [
            "WITH x AS (TABLE other) SELECT * FROM x",
            "CREATE TABLE c AS TABLE other",
            r#"WITH x AS (TABLE "SECRETS") SELECT * FROM x"#,
        ] {
            assert!(!refused(sql).await, "{}", sql);
        }
    }

    #[tokio::test]
    async fn test_every_table_grants_are_refused_only_with_a_forbidden_table() {
        for sql in [
            "GRANT SELECT ON ALL TABLES IN SCHEMA public TO r",
            "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC",
        ] {
            assert!(!refused_by(&QueryRouter::new(), &[], sql).await, "{}", sql);
        }
    }

    #[test]
    fn test_table_access_struct_creation() {
        let tables = vec!["secret_table".to_string()];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert!(ta.enabled);
        assert_eq!(ta.tables.len(), 1);
    }

    #[test]
    fn test_table_access_disabled() {
        let tables = vec!["secret_table".to_string()];
        let ta = TableAccess {
            enabled: false,
            tables: &tables,
        };
        assert!(!ta.enabled);
    }

    #[test]
    fn test_table_access_with_multiple_tables() {
        let tables = vec![
            "users".to_string(),
            "passwords".to_string(),
            "secrets".to_string(),
        ];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert_eq!(ta.tables.len(), 3);
        assert!(ta.tables.contains(&"users".to_string()));
        assert!(ta.tables.contains(&"passwords".to_string()));
        assert!(ta.tables.contains(&"secrets".to_string()));
    }

    #[test]
    fn test_table_access_empty_tables() {
        let tables: Vec<String> = vec![];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert!(ta.tables.is_empty());
    }
}
