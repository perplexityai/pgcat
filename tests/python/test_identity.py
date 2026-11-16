"""
End-to-end tests for SET / RESET / SHOW PGCAT_IDENTITY and the tenant parameter.

The identity is checked through the test-only `testing` plugin, which also
verifies tokens: `tenant:<id>` names a tenant, which pgcat puts in app.tenant
on the server connection. These tests are skipped unless PGCAT_TEST_PLUGINS=1
(see test_plugin_hooks.py).
"""

import asyncio
import hashlib
import os
import re
import struct
import time
import uuid

import asyncpg
import psycopg
import psycopg2
import pytest

import utils

pytestmark = pytest.mark.skipif(
    os.environ.get("PGCAT_TEST_PLUGINS") != "1",
    reason="needs pgcat built with --features test-plugins and PGCAT_TEST_PLUGINS=1",
)

PORT = 6441
DB = "identity_db"
SESSION_DB = "identity_session_db"
# No tenant parameter, parser or plugins: pgcat still keeps the token from the server.
PLAIN_DB = "identity_plain_db"

CONFIG = f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"

[pools.{DB}]
pool_mode = "transaction"
prepared_statements_cache_size = 100
query_parser_enabled = true
plugins_require_parse = true

identity_tenant_parameter = "app.tenant"

[pools.{DB}.plugins.testing]
enabled = true
whoami = true
verify_identity = true

[pools.{DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{DB}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"

[pools.{SESSION_DB}]
pool_mode = "session"
query_parser_enabled = true
plugins_require_parse = true
identity_tenant_parameter = "app.tenant"

[pools.{SESSION_DB}.plugins.testing]
enabled = true
whoami = true
verify_identity = true

[pools.{SESSION_DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{SESSION_DB}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"

[pools.{PLAIN_DB}]
pool_mode = "transaction"

[pools.{PLAIN_DB}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{PLAIN_DB}.shards.0]
servers = [["127.0.0.1", 5432, "primary"]]
database = "shard0"
"""

# Every token used here, to check none of them reached the server or the logs.
TOKENS = []


def token(name):
    value = f"{name}-{uuid.uuid4()}"
    TOKENS.append(value)
    return value


# Fingerprint shown for each token. They're keyed per pgcat process, so the
# tests learn them instead of computing them.
FINGERPRINTS = {}


def shows(rows, value):
    """Whether SHOW PGCAT_IDENTITY returned this token's fingerprint."""
    [(is_set, shown)] = rows
    assert is_set is True and re.fullmatch("[0-9a-f]{12}", shown)
    # Not something a reader of the logs could compute from a guessed token.
    assert shown != hashlib.sha256(value.encode()).hexdigest()[:12]
    expected = FINGERPRINTS.setdefault(value, shown)
    return shown == expected and list(FINGERPRINTS.values()).count(shown) == 1


def superuser():
    conn = psycopg2.connect("postgres://postgres:postgres@127.0.0.1:5432/shard0")
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def pgcat():
    conn = superuser()
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    cur.execute("SELECT pg_stat_statements_reset()")
    # Functions pgcat can't look into: one sets the tenant parameter, and one
    # reads it without the query naming it, so reading it doesn't make pgcat
    # set it again first.
    cur.execute(
        "CREATE OR REPLACE FUNCTION set_app_tenant(value text) RETURNS text "
        "LANGUAGE sql AS $$ SELECT set_config('app.tenant', value, false) $$"
    )
    cur.execute(
        "CREATE OR REPLACE FUNCTION current_org() RETURNS text "
        "LANGUAGE sql AS $$ SELECT current_setting('app.tenant', true) $$"
    )
    # Sets it, and calling it doesn't name it either.
    cur.execute(
        "CREATE OR REPLACE FUNCTION innocent() RETURNS text "
        "LANGUAGE sql AS $$ SELECT set_config('app.tenant', 'intruder', false) $$"
    )
    conn.close()

    # Trace logging, so the redaction check covers every log line.
    process = utils.PgcatProcess(CONFIG, PORT, log_level="trace").start()
    yield process
    process.stop()


def simple_conn(pgcat):
    conn = psycopg2.connect(pgcat.dsn(DB))
    conn.autocommit = True
    return conn, conn.cursor()


def whoami(cur):
    cur.execute("SELECT 'whoami'")
    return cur.fetchone()[0]


def tenant_token(name):
    """A token the testing plugin verifies as naming tenant `name`."""
    value = f"tenant:{name}#{uuid.uuid4()}"
    TOKENS.append(value)
    return value


def server_tenant(cur):
    """app.tenant as the server sees it. Never set reads as NULL, reset as ''."""
    cur.execute("SELECT current_setting('app.tenant', true)")
    return cur.fetchone()[0] or None


def test_set_show_reset_simple_protocol(pgcat):
    conn, cur = simple_conn(pgcat)

    cur.execute("SHOW PGCAT_IDENTITY")
    assert [column.name for column in cur.description] == [
        "identity_set",
        "identity_fingerprint",
    ]
    assert cur.fetchall() == [(False, None)]
    assert whoami(cur) is None

    value = token("simple")
    cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
    cur.execute("SHOW PGCAT_IDENTITY")
    assert shows(cur.fetchall(), value)
    assert whoami(cur) == value

    # Regular queries still work and go to the server.
    cur.execute("SELECT 1 + 1")
    assert cur.fetchone() == (2,)

    cur.execute("RESET PGCAT_IDENTITY")
    cur.execute("SHOW PGCAT_IDENTITY")
    assert cur.fetchall() == [(False, None)]
    assert whoami(cur) is None

    # Comments and quoted identifiers don't get the command past pgcat.
    for template in [
        "SET/**/PGCAT_IDENTITY TO '{}'",
        "SET\n-- comment\nPGCAT_IDENTITY TO '{}'",
        'SET "pgcat_identity" TO \'{}\'',
    ]:
        value = token("spelling")
        cur.execute(template.format(value))
        assert whoami(cur) == value

    # A value that mentions the command is just a value.
    cur.execute("SELECT 'SET pgcat_identity first'")
    assert cur.fetchone() == ("SET pgcat_identity first",)
    conn.close()


def test_set_show_reset_extended_protocol(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        value = token("psycopg")
        conn.execute(f"SET PGCAT_IDENTITY TO '{value}'")
        assert shows(conn.execute("SHOW PGCAT_IDENTITY").fetchall(), value)
        assert conn.execute("SELECT 'whoami'").fetchone() == (value,)

        # Prepared and reused.
        for _ in range(3):
            assert shows([conn.execute("SHOW PGCAT_IDENTITY", prepare=True).fetchone()], value)

        conn.execute("RESET PGCAT_IDENTITY")
        assert conn.execute("SELECT 'whoami'").fetchone() == (None,)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            value = token("asyncpg")
            # Simple protocol for execute() without arguments...
            await conn.execute(f"SET PGCAT_IDENTITY TO '{value}'")
            # ...and Parse, Describe, Flush, Bind, Execute, Sync for fetch.
            row = await conn.fetchrow("SHOW PGCAT_IDENTITY")
            assert shows([tuple(row)], value)
            assert await conn.fetchval("SELECT 'whoami'") == value

            stmt = await conn.prepare("RESET PGCAT_IDENTITY")
            await stmt.fetch()
            # SHOW runs at Execute. (whoami is answered at Parse, and asyncpg
            # reuses the statement it prepared above.)
            assert tuple(await conn.fetchrow("SHOW PGCAT_IDENTITY")) == (False, None)
        finally:
            await conn.close()

    asyncio.run(run())


def test_set_inside_transaction(pgcat):
    conn, cur = simple_conn(pgcat)
    value = token("transaction")

    cur.execute("BEGIN")
    cur.execute("SELECT 1")
    cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
    assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS
    assert whoami(cur) == value
    cur.execute("COMMIT")

    assert whoami(cur) == value
    conn.close()

    with psycopg.connect(pgcat.dsn(DB)) as conn:
        value = token("psycopg-transaction")
        conn.execute("SELECT 1")
        conn.execute(f"SET PGCAT_IDENTITY TO '{value}'")
        assert conn.execute("SELECT 'whoami'").fetchone() == (value,)
        conn.commit()


def test_clients_sharing_a_server_keep_their_identity(pgcat):
    # pool_size is 1 and the pool is in transaction mode: both clients use the
    # same server connection, one transaction at a time.
    conn_a, cur_a = simple_conn(pgcat)
    conn_b, cur_b = simple_conn(pgcat)

    cur_a.execute("SELECT pg_backend_pid()")
    cur_b.execute("SELECT pg_backend_pid()")
    assert cur_a.fetchone() == cur_b.fetchone()

    value_a, value_b = token("client-a"), token("client-b")
    cur_a.execute(f"SET PGCAT_IDENTITY TO '{value_a}'")
    cur_b.execute(f"SET PGCAT_IDENTITY TO '{value_b}'")

    for _ in range(3):
        assert whoami(cur_a) == value_a
        assert whoami(cur_b) == value_b

    cur_b.execute("RESET PGCAT_IDENTITY")
    assert whoami(cur_a) == value_a
    assert whoami(cur_b) is None

    conn_a.close()
    conn_b.close()

    # A new client starts without an identity.
    conn, cur = simple_conn(pgcat)
    assert whoami(cur) is None
    conn.close()


def test_discard_all_clears_identity(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{token('discard')}'")
    cur.execute("DISCARD ALL")
    assert whoami(cur) is None

    cur.execute(f"SET PGCAT_IDENTITY TO '{token('reset-all')}'")
    cur.execute("RESET ALL")
    assert whoami(cur) is None
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute(f"SET PGCAT_IDENTITY TO '{token('psycopg-discard')}'")
        conn.execute("DISCARD ALL")
        assert conn.execute("SELECT 'whoami'").fetchone() == (None,)


def test_identity_mixed_with_other_sql_is_refused(pgcat):
    conn, cur = simple_conn(pgcat)
    with pytest.raises(psycopg2.Error, match="must be sent on their own"):
        cur.execute(f"SET PGCAT_IDENTITY TO '{token('mixed')}'; SELECT 1")
    with pytest.raises(psycopg2.Error, match="must be sent on their own"):
        cur.execute(f"SET LOCAL PGCAT_IDENTITY TO '{token('local')}'")
    assert whoami(cur) is None
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="must be sent on their own"):
            conn.execute(f"SELECT 1; SET PGCAT_IDENTITY TO '{token('mixed-extended')}'")
        assert conn.execute("SELECT 'whoami'").fetchone() == (None,)


def test_autocommit_statements_carry_identity(pgcat):
    # Transaction mode: every autocommit statement checks a server connection out
    # on its own, and each one carries the identity without a BEGIN.
    conn, cur = simple_conn(pgcat)
    value = tenant_token("acme")
    cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")

    for _ in range(3):
        assert conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_IDLE
        assert server_tenant(cur) == "acme"
        assert whoami(cur) == value
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        value = tenant_token("acme-extended")
        conn.execute(f"SET PGCAT_IDENTITY TO '{value}'")
        for _ in range(3):
            row = conn.execute("SELECT current_setting('app.tenant', true)").fetchone()
            assert row == ("acme-extended",)

    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('acme-asyncpg')}'")
            for _ in range(3):
                assert (
                    await conn.fetchval("SELECT current_setting('app.tenant', true)")
                    == "acme-asyncpg"
                )
        finally:
            await conn.close()

    asyncio.run(run())


def test_pooled_connection_handoff(pgcat):
    # One server connection, used by three clients in turn: each sees its own
    # tenant, or none, never the previous client's.
    conn_a, cur_a = simple_conn(pgcat)
    conn_b, cur_b = simple_conn(pgcat)
    cur_a.execute("SELECT pg_backend_pid()")
    cur_b.execute("SELECT pg_backend_pid()")
    assert cur_a.fetchone() == cur_b.fetchone()

    cur_a.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('tenant-a')}'")
    assert server_tenant(cur_a) == "tenant-a"
    assert server_tenant(cur_b) is None

    cur_b.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('tenant-b')}'")
    for _ in range(3):
        assert server_tenant(cur_a) == "tenant-a"
        assert server_tenant(cur_b) == "tenant-b"

    # A token without a tenant clears it on the server too.
    cur_b.execute(f"SET PGCAT_IDENTITY TO '{token('no-tenant')}'")
    assert server_tenant(cur_b) is None
    assert server_tenant(cur_a) == "tenant-a"

    # Inside a transaction the connection stays with A, then goes back to the pool.
    cur_a.execute("BEGIN")
    assert server_tenant(cur_a) == "tenant-a"
    cur_a.execute("COMMIT")
    assert server_tenant(cur_b) is None

    conn_a.close()
    conn_b.close()

    conn, cur = simple_conn(pgcat)
    assert server_tenant(cur) is None
    conn.close()


def test_row_level_security_sees_the_tenant(pgcat):
    # A policy that reads the parameter the way an application's would, on a
    # table sharding_user (not a superuser, so RLS applies) can read.
    admin = superuser()
    admin_cur = admin.cursor()
    admin_cur.execute("DROP TABLE IF EXISTS identity_rls")
    admin_cur.execute("CREATE TABLE identity_rls (tenant text, secret text)")
    admin_cur.execute("INSERT INTO identity_rls VALUES ('acme', 'a'), ('globex', 'g')")
    admin_cur.execute("ALTER TABLE identity_rls ENABLE ROW LEVEL SECURITY")
    admin_cur.execute(
        "CREATE POLICY tenant_rows ON identity_rls "
        "USING (tenant = NULLIF(current_setting('app.tenant', true), ''))"
    )
    admin_cur.execute("GRANT SELECT ON identity_rls TO sharding_user")

    def visible(cur):
        cur.execute("SELECT secret FROM identity_rls ORDER BY secret")
        return [row[0] for row in cur.fetchall()]

    try:
        conn_a, cur_a = simple_conn(pgcat)
        conn_b, cur_b = simple_conn(pgcat)
        assert visible(cur_a) == []

        cur_a.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('acme')}'")
        cur_b.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('globex')}'")

        # Autocommit, the two clients taking turns on the one server connection.
        for _ in range(2):
            assert visible(cur_a) == ["a"]
            assert visible(cur_b) == ["g"]

        # In a transaction, and after it.
        cur_a.execute("BEGIN")
        assert visible(cur_a) == ["a"]
        assert visible(cur_a) == ["a"]
        cur_a.execute("COMMIT")
        assert visible(cur_b) == ["g"]
        assert visible(cur_a) == ["a"]

        # Extended protocol.
        with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
            conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('globex')}'")
            query = "SELECT secret FROM identity_rls"
            assert conn.execute(query).fetchall() == [("g",)]
            with conn.transaction():
                assert conn.execute(query).fetchall() == [("g",)]
            assert conn.execute(query).fetchall() == [("g",)]

        # Without an identity, the policy sees no tenant.
        cur_a.execute("RESET PGCAT_IDENTITY")
        assert visible(cur_a) == []
        assert visible(cur_b) == ["g"]

        conn_a.close()
        conn_b.close()
    finally:
        admin_cur.execute("DROP TABLE identity_rls")
        admin.close()


def test_reset_and_discard_clear_identity_and_tenant(pgcat):
    conn, cur = simple_conn(pgcat)

    for reset in ["RESET PGCAT_IDENTITY", "DISCARD ALL", "RESET ALL"]:
        value = tenant_token("resettable")
        cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
        assert server_tenant(cur) == "resettable"
        cur.execute(reset)
        assert whoami(cur) is None
        assert server_tenant(cur) is None

    # A new SET replaces the identity and its tenant.
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('first')}'")
    second = tenant_token("second")
    cur.execute(f"SET PGCAT_IDENTITY TO '{second}'")
    assert whoami(cur) == second
    assert server_tenant(cur) == "second"
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('extended-reset')}'")
        conn.execute("RESET PGCAT_IDENTITY")
        assert conn.execute("SELECT current_setting('app.tenant', true)").fetchone()[0] in (
            None,
            "",
        )


@pytest.mark.parametrize("db", [DB, SESSION_DB])
def test_commit_and_rollback_keep_identity(pgcat, db):
    conn = psycopg2.connect(pgcat.dsn(db))
    conn.autocommit = True
    cur = conn.cursor()

    first = tenant_token("kept")
    cur.execute(f"SET PGCAT_IDENTITY TO '{first}'")
    for end in ["COMMIT", "ROLLBACK"]:
        cur.execute("BEGIN")
        assert server_tenant(cur) == "kept"
        cur.execute(end)
        assert whoami(cur) == first
        assert server_tenant(cur) == "kept"

    # Set inside a transaction that's rolled back: the identity stays, and the
    # server's parameter is set again even though the ROLLBACK undid it.
    for end in ["ROLLBACK", "COMMIT"]:
        cur.execute("BEGIN")
        cur.execute("SELECT 1")
        value = tenant_token(f"inside-{end.lower()}")
        cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
        assert server_tenant(cur) == f"inside-{end.lower()}"
        cur.execute(end)
        assert whoami(cur) == value
        assert server_tenant(cur) == f"inside-{end.lower()}"

    # Also when the transaction failed first.
    cur.execute("BEGIN")
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT 1/0")
    value = tenant_token("after-failure")
    cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
    cur.execute("ROLLBACK")
    assert server_tenant(cur) == "after-failure"
    conn.close()


def test_clients_cannot_set_the_tenant(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")

    for query in [
        "SET app.tenant TO 'intruder'",
        "SET LOCAL app.tenant TO 'intruder'",
        'SET "app"."tenant" = \'intruder\'',
        "RESET app.tenant",
        "SELECT set_config('app.tenant', 'intruder', false)",
        "SELECT set_config('app.' || 'tenant', 'intruder', false)",
        "DO $$ BEGIN PERFORM set_config('app.tenant', 'intruder', false); END $$",
        "DO $$ BEGIN EXECUTE 'SELECT set_' || 'config(''app.tenant'', ''intruder'', false)'; END $$",
        # Bodies have their own quoting and escapes: pgcat doesn't read them.
        'DO $$BEGIN SET "app.tenant" = \'intruder\'; END$$',
        "CREATE FUNCTION pg_temp.f() RETURNS void LANGUAGE sql "
        'AS $$ SET "app"."tenant" = \'intruder\' $$',
        "DO E'BEGIN SET app.\\x74enant = \\'intruder\\'; END'",
        "DO $$ BEGIN RAISE NOTICE 'harmless'; END $$",
        # Unicode escapes spell the name, or set_config, without writing it.
        'SET U&"app.tenant" TO \'intruder\'',
        'SET U&"app\\002etenant" TO \'intruder\'',
        'SET U&"\\0061\\0070p\\002e\\0074enant" TO \'intruder\'',
        'SET U&"app!002etenant" UESCAPE \'!\' TO \'intruder\'',
        'SELECT U&"\\0073et_config"(U&\'app\\002etenant\', \'intruder\', false)',
    ]:
        with pytest.raises(psycopg2.Error, match="set by pgcat"):
            cur.execute(query)
        assert server_tenant(cur) == "owner"

    # Reading it, and other parameters, is fine.
    cur.execute("SELECT set_config('application_name', 'still-fine', false)")
    assert server_tenant(cur) == "owner"
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner-extended')}'")
        with pytest.raises(psycopg.Error, match="set by pgcat"):
            conn.execute("SELECT set_config('app.tenant', %s, false)", ["intruder"])
        with pytest.raises(psycopg.Error, match="Unicode escapes"):
            conn.execute(
                'SELECT U&"\\0073et_config"(U&\'\\0061\\0070p\\002e\\0074enant\', %s, false)',
                ["intruder"],
            )
        row = conn.execute("SELECT current_setting('app.tenant', true)").fetchone()
        assert row == ("owner-extended",)

    # Nor does the next client on that server connection get anyone's value
    # but its own.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    assert server_tenant(cur) == "owner"
    conn.close()


@pytest.mark.parametrize(
    "query",
    [
        # Each is a string to Postgres, and the SET after it runs.
        "SELECT name'\\', ' -- '; SET app.tenant TO 'intruder'",
        "SELECT 1 AS a$$, '$$ -- '; SET app.tenant TO 'intruder'",
        "SELECT 1 -- a\rSET app.tenant TO 'intruder'",
        "SELECT E'a'\n'\\' -- '; SET app.tenant TO 'intruder'",
    ],
    ids=["type-name", "dollar-in-identifier", "carriage-return", "string-continuation"],
)
def test_comment_tricks_cannot_hide_a_set(pgcat, query):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    with pytest.raises(psycopg2.Error, match="set by pgcat") as refused:
        cur.execute(query)
    assert refused.value.pgcode == "42501"
    assert server_tenant(cur) == "owner"
    conn.close()


def test_clients_cannot_change_how_strings_are_read(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")

    for query in [
        "SET standard_conforming_strings = off",
        "SELECT set_config('standard_conforming_strings', 'off', false)",
        "SET client_encoding TO 'SJIS'",
        "SET NAMES 'SJIS'",
    ]:
        with pytest.raises(psycopg2.Error, match="standard_conforming_strings") as refused:
            cur.execute(query)
        assert refused.value.pgcode == "42501"

    # What drivers send is fine.
    cur.execute("SET client_encoding TO 'UTF8'")
    cur.execute("SET NAMES 'utf-8'")
    cur.execute("SHOW standard_conforming_strings")
    assert cur.fetchone() == ("on",)
    conn.close()


def test_sessions_pgcat_cannot_read_are_refused(pgcat):
    # Startup parameters are set on the server connection before the client's
    # statements, so they never pass the guard.
    with pytest.raises(psycopg2.Error, match="client_encoding UTF8"):
        psycopg2.connect(pgcat.dsn(DB), client_encoding="SJIS")

    async def run():
        with pytest.raises(asyncpg.FeatureNotSupportedError, match="standard_conforming_strings"):
            await asyncpg.connect(
                pgcat.dsn(DB), server_settings={"standard_conforming_strings": "off"}
            )

    asyncio.run(run())

    # Nor did the attempts change the server connection.
    conn, cur = simple_conn(pgcat)
    cur.execute("SHOW standard_conforming_strings")
    assert cur.fetchone() == ("on",)
    conn.close()


def current_org(cur):
    cur.execute("SELECT current_org()")
    return cur.fetchone()[0]


@pytest.mark.parametrize("first", [True, False], ids=["first-statement", "in-transaction"])
def test_tenant_changed_behind_pgcat_is_set_again(pgcat, first):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    if not first:
        cur.execute("BEGIN")
    cur.execute("SELECT set_app_tenant('intruder')")
    assert current_org(cur) == "owner"
    if not first:
        cur.execute("COMMIT")
    conn.close()

    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
        conn.execute("SELECT set_app_tenant(%s)", ["intruder"])
        assert conn.execute("SELECT current_org()").fetchone() == ("owner",)

    # The next client with that tenant on the same server connection.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    cur.execute("SELECT set_app_tenant('intruder')")
    conn.close()
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    assert current_org(cur) == "owner"
    conn.close()


def test_prepared_statement_is_caught_each_time_it_runs(pgcat):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
            # Parse, Describe and Flush now; Bind, Execute and Sync each time it runs.
            forge = await conn.prepare("SELECT set_app_tenant($1)")
            for _ in range(2):
                await forge.fetchval("intruder")
                assert await conn.fetchval("SELECT current_org()") == "owner"
                await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
            await forge.fetchval("intruder")
        finally:
            await conn.close()

    asyncio.run(run())

    # The next client with that tenant on the same server connection.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    assert current_org(cur) == "owner"
    conn.close()


def test_tenant_changed_without_naming_it_is_set_again_at_checkout(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    cur.execute("SELECT innocent()")
    # Each autocommit statement checks the server connection out again.
    assert current_org(cur) == "owner"
    cur.execute("SELECT innocent()")
    conn.close()

    # The next client with that tenant on the same server connection.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('owner')}'")
    assert current_org(cur) == "owner"
    conn.close()


def test_reset_all_counts_when_it_runs(pgcat):
    async def run():
        conn = await asyncpg.connect(pgcat.dsn(DB))
        try:
            await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")

            # Prepared, not run: the identity stays.
            reset = await conn.prepare("RESET ALL")
            assert (await conn.fetchrow("SHOW PGCAT_IDENTITY"))[0] is True

            # Run, and run again without a new Parse: it goes each time.
            for _ in range(2):
                await reset.fetch()
                assert (await conn.fetchrow("SHOW PGCAT_IDENTITY"))[0] is False
                await conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
        finally:
            await conn.close()

    asyncio.run(run())

    # The server connection's tenant was reset by it: the next client with that
    # tenant gets it set again.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
    assert current_org(cur) == "resetter"
    conn.close()

    # Parse, Bind, Execute and Sync in one batch.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
        conn.execute("RESET ALL")
        assert conn.execute("SHOW PGCAT_IDENTITY").fetchone()[0] is False

    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('resetter')}'")
    assert current_org(cur) == "resetter"
    conn.close()


def test_reset_all_counts_when_executed(pgcat):
    sock, message = utils.raw_connection(PORT, DB)
    frontend = utils.frontend

    def replies(messages):
        sock.sendall(messages)
        codes = []
        while not codes or codes[-1] != b"Z":
            codes.append(message()[0])
        return codes

    def query(sql):
        return replies(frontend(b"Q", sql.encode() + b"\0"))

    def identity_set():
        sock.sendall(frontend(b"Q", b"SHOW PGCAT_IDENTITY\0"))
        while True:
            code, body = message()
            if code == b"D":
                # The first column: its length, then its text.
                (length,) = struct.unpack("!i", body[2:6])
                row = body[6 : 6 + length]
            if code == b"Z":
                return row == b"t"

    parse = frontend(b"P", b"r\0RESET ALL\0" + struct.pack("!h", 0))
    bind = frontend(b"B", b"p\0r\0" + struct.pack("!hhh", 0, 0, 0))
    execute = frontend(b"E", b"p\0" + struct.pack("!i", 0))
    sync = frontend(b"S", b"")

    query(f"SET PGCAT_IDENTITY TO '{tenant_token('portal')}'")

    # Bound, not executed: the identity stays.
    assert replies(parse + bind + sync) == [b"1", b"2", b"Z"]
    assert identity_set()

    # Bound in one batch and executed in the next (a transaction keeps the
    # portal): the identity goes with the second.
    query("BEGIN")
    sock.sendall(bind + frontend(b"H", b""))
    assert [message()[0]] == [b"2"]
    assert identity_set()
    assert replies(execute + sync) == [b"C", b"Z"]
    assert not identity_set()
    query("COMMIT")
    sock.close()

    # And the server connection's tenant was reset with it.
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('portal')}'")
    assert current_org(cur) == "portal"
    conn.close()


@pytest.mark.parametrize("name", ['U&"pgcat_identity"', 'U&"pgcat\\005fidentity"'])
def test_unicode_spelling_keeps_the_token_from_the_server(pgcat, name):
    # Simple protocol...
    conn = psycopg2.connect(pgcat.dsn(PLAIN_DB))
    conn.autocommit = True
    with pytest.raises(psycopg2.Error, match="Unicode escapes") as refused:
        conn.cursor().execute(f"SET {name} TO '{token('unicode-simple')}'")
    assert refused.value.pgcode == "0A000"
    conn.close()

    # ...and extended. The last test checks the tokens never reached the server.
    with psycopg.connect(pgcat.dsn(PLAIN_DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="Unicode escapes") as refused:
            conn.execute(f"SET {name} TO '{token('unicode-extended')}'", prepare=True)
        assert refused.value.sqlstate == "0A000"
        assert conn.execute("SELECT 1").fetchone() == (1,)


class ServerLog:
    """What Postgres logs from now on, where the test can read its log: the
    file PGCAT_TEST_SERVER_LOG names, or the logging collector's. Read through
    the server, which can read it when the test can't."""

    def __init__(self):
        self.conn = superuser()
        self.path = os.environ.get("PGCAT_TEST_SERVER_LOG") or self.query(
            "SELECT pg_current_logfile()"
        )
        self.start = self.size() if self.path else None

    def query(self, sql, *args):
        cur = self.conn.cursor()
        cur.execute(sql, args)
        return cur.fetchone()[0]

    def size(self):
        return self.query("SELECT size FROM pg_stat_file(%s)", self.path)

    def read(self):
        length = self.size() - self.start
        return self.query("SELECT pg_read_file(%s, %s, %s)", self.path, self.start, length)

    def wait_for(self, text, timeout=10.0):
        """The log once it has `text`, or fail after `timeout` seconds."""
        deadline = time.monotonic() + timeout
        while True:
            logged = self.read()
            if text in logged:
                return logged
            assert time.monotonic() < deadline, f"{text!r} wasn't logged in {timeout}s"
            time.sleep(0.05)


# Ways to call set_config() with pgcat_identity. With a name that isn't a plain
# literal, in a query that names it or has Unicode escapes, pgcat can't tell it
# apart, so it's refused too.
SET_CONFIG_IDENTITY = [
    "SELECT set_config('pgcat_identity', '{token}', false)",
    "select SET_CONFIG( ' PGCAT_IDENTITY ' , '{token}', true)",
    "SELECT * FROM set_config('pgcat_identity', '{token}', false)",
    "SELECT pg_catalog . \"set_config\" /* c */ ('pgcat_identity', '{token}', false)",
    "SELECT set_config(E'pgcat_identity', '{token}', false)",
    "SELECT set_config(U&'pgcat\\005fidentity', '{token}', false)",
    "DO $$BEGIN PERFORM set_config('pgcat_identity', '{token}', false); END$$",
]


def error_fields(body):
    """An ErrorResponse's fields, by their one-letter codes."""
    return {
        field[:1].decode(): field[1:].decode() for field in body.rstrip(b"\0").split(b"\0")
    }


def test_set_config_of_the_identity_is_refused(pgcat):
    log = ServerLog()
    frontend = utils.frontend
    sent = []

    def refused(db, messages):
        """Send `messages` on a new connection: pgcat refuses them with one
        ErrorResponse, and the connection still works."""
        sock, message = utils.raw_connection(PORT, db)
        sock.settimeout(10)
        sock.sendall(messages)
        replies = [message() for _ in range(2)]
        assert [code for code, _ in replies] == [b"E", b"Z"], replies
        fields = error_fields(replies[0][1])
        sock.sendall(frontend(b"Q", b"SELECT 1\0"))
        codes = [message()[0] for _ in range(4)]
        assert codes == [b"T", b"D", b"C", b"Z"]
        sock.close()
        return fields

    # Without the tenant parameter's guard, and with it, which refuses some of
    # them first.
    for db, queries in [(PLAIN_DB, SET_CONFIG_IDENTITY), (DB, SET_CONFIG_IDENTITY[:4])]:
        for template in queries:
            value = token("set-config")
            query = template.format(token=value).encode() + b"\0"
            sent.append(value)

            # Simple protocol...
            fields = refused(db, frontend(b"Q", query))
            assert (fields["S"], fields["C"]) == ("ERROR", "0A000"), (template, fields)
            assert "can't be set with set_config()" in fields["M"]
            assert value not in fields["M"]

            # ...and extended: Parse, Bind, Execute, Sync in one batch.
            fields = refused(
                db,
                frontend(b"P", b"\0" + query + struct.pack("!h", 0))
                + frontend(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
                + frontend(b"E", b"\0" + struct.pack("!i", 0))
                + frontend(b"S", b""),
            )
            assert (fields["S"], fields["C"]) == ("ERROR", "0A000"), (template, fields)
            assert "can't be set with set_config()" in fields["M"]

    if log.path is None:
        # The server log isn't readable here (stderr): the last test checks
        # pg_stat_statements, and the SQLSTATEs above are pgcat's, not the
        # server's 42704 for a setting it doesn't know.
        return

    # The server logs a statement it fails, as it would these: once it has
    # logged one sent straight to it after them, it would have logged them.
    canary = f"canary-{uuid.uuid4()}"
    direct = psycopg2.connect("postgres://sharding_user:sharding_user@127.0.0.1:5432/shard0")
    with pytest.raises(psycopg2.Error) as unknown:
        direct.cursor().execute(f"SELECT set_config('pgcat_identity', '{canary}', false)")
    assert unknown.value.pgcode == "42704"
    direct.close()
    logged = log.wait_for(canary)
    for value in sent:
        assert value not in logged


def test_a_name_with_backslash_escapes_is_skipped(pgcat):
    """A form the set_config() check skips: with standard_conforming_strings
    off, pgcat doesn't read a backslash escape as Postgres does, so the server
    gets the call. Only a pool without the tenant parameter lets a client turn
    it off. The value isn't a token."""
    escaped = "SELECT set_config('pgcat\\_identity', 'not-a-token', false)"
    plain = psycopg2.connect(pgcat.dsn(PLAIN_DB))
    cur = plain.cursor()
    cur.execute("SET LOCAL standard_conforming_strings = off")
    with pytest.raises(psycopg2.Error) as unknown:
        cur.execute(escaped)
    assert unknown.value.pgcode == "42704"
    plain.rollback()
    plain.close()

    conn, cur = simple_conn(pgcat)
    with pytest.raises(psycopg2.Error) as refused:
        cur.execute("SET standard_conforming_strings = off")
    assert refused.value.pgcode == "42501"
    conn.close()


def test_refused_token_clears_identity(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('before')}'")
    with pytest.raises(psycopg2.Error, match="PGCAT_IDENTITY refused"):
        cur.execute(f"SET PGCAT_IDENTITY TO '{token('invalid')}'")
    assert whoami(cur) is None
    assert server_tenant(cur) is None

    # A tenant pgcat can't put in a parameter is refused too.
    with pytest.raises(psycopg2.Error, match="PGCAT_IDENTITY refused"):
        cur.execute("SET PGCAT_IDENTITY TO 'tenant:a b'")
    conn.close()


@pytest.mark.parametrize("prepare", [False, True], ids=["simple", "extended"])
@pytest.mark.parametrize(
    "query, sqlstate, message",
    [
        ("SET PGCAT_IDENTITY TO '{invalid}'", "28000", "PGCAT_IDENTITY refused"),
        ("SELECT set_config('app.tenant', 'intruder', false)", "42501", "set by pgcat"),
        (
            "SELECT U&\"\\0073et_config\"(U&'\\0061\\0070p\\002e\\0074enant', 'intruder', false)",
            "42501",
            "Unicode escapes",
        ),
        ("SET LOCAL PGCAT_IDENTITY TO '{invalid}'", "0A000", "must be sent on their own"),
    ],
    ids=["refused-token", "tenant-parameter", "unicode-escapes", "unsupported"],
)
def test_refusals_keep_their_sqlstate(pgcat, prepare, query, sqlstate, message):
    # Without a SQLSTATE, the refusal would be a FATAL, and drivers like pgx
    # drop the connection on one.
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match=message) as refused:
            conn.execute(query.format(invalid=token("invalid")), prepare=prepare)
        assert refused.value.sqlstate == sqlstate
        assert refused.value.diag.severity_nonlocalized == "ERROR"
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_expired_identity_is_forgotten(pgcat):
    conn, cur = simple_conn(pgcat)
    value = f"expires:{int(time.time()) + 2}:tenant:short-lived#{uuid.uuid4()}"
    TOKENS.append(value)
    cur.execute(f"SET PGCAT_IDENTITY TO '{value}'")
    assert server_tenant(cur) == "short-lived"
    time.sleep(3)
    assert whoami(cur) is None
    assert server_tenant(cur) is None
    conn.close()


def test_rollback_to_savepoint_resets_the_tenant(pgcat):
    conn, cur = simple_conn(pgcat)
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('before-savepoint')}'")
    assert server_tenant(cur) == "before-savepoint"

    cur.execute("BEGIN")
    cur.execute("SAVEPOINT s")
    cur.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('after-savepoint')}'")
    assert server_tenant(cur) == "after-savepoint"
    # Undoes pgcat's SET on the server; pgcat sets it again.
    cur.execute("ROLLBACK TO SAVEPOINT s")
    assert server_tenant(cur) == "after-savepoint"
    cur.execute("COMMIT")
    assert server_tenant(cur) == "after-savepoint"
    conn.close()


def test_rejected_batch_leaves_the_identity(pgcat):
    with psycopg.connect(pgcat.dsn(DB), autocommit=True) as conn:
        first = tenant_token("kept")
        conn.execute(f"SET PGCAT_IDENTITY TO '{first}'")
        assert shows(conn.execute("SHOW PGCAT_IDENTITY").fetchall(), first)

        # Answered by pgcat and the server in one batch: refused, so the SET
        # never happened.
        with pytest.raises(psycopg.Error, match="their own Sync batch"):
            with conn.pipeline():
                conn.execute(f"SET PGCAT_IDENTITY TO '{tenant_token('dropped')}'")
                conn.execute("SELECT 1")
        assert shows(conn.execute("SHOW PGCAT_IDENTITY").fetchall(), first)
        assert conn.execute("SELECT current_setting('app.tenant', true)").fetchone() == ("kept",)

        # Nor did a DISCARD ALL in a rejected batch.
        with pytest.raises(psycopg.Error, match="their own Sync batch"):
            with conn.pipeline():
                conn.execute("DISCARD ALL")
                conn.execute("SHOW PGCAT_IDENTITY")
        assert shows(conn.execute("SHOW PGCAT_IDENTITY").fetchall(), first)


def test_token_never_reaches_server_or_logs(pgcat):
    assert TOKENS, "run with the other tests in this module"

    conn = superuser()
    cur = conn.cursor()
    cur.execute("SELECT query FROM pg_stat_statements")
    statements = "\n".join(row[0] for row in cur.fetchall())
    conn.close()

    # The server did see other queries from these tests.
    assert "pg_backend_pid" in statements
    assert "PGCAT_IDENTITY" not in statements.upper()

    logs = pgcat.logs()
    assert "Caller identity set" in logs
    assert "Caller identity refused" in logs
    assert "Caller identity expired" in logs
    for value in TOKENS:
        assert value not in statements
        assert value not in logs

