"""
Raw-SQL scenarios in tests/psql/, run with psql through pgcat.

Each scenario runs twice: once directly against Postgres and once through
pgcat. The two transcripts (stdout and stderr, as psql prints them) must be
identical, because a pooler that is correct is invisible to a client. A
scenario with a `<name>.out` file next to it is compared to that file instead;
those cover behavior that is pgcat's own (for example how a transaction pool
treats session SETs), where Postgres is not the reference.

Every pool here has pool_size = 1, and every scenario gets a fresh pgcat, so each `\\connect` in a scenario makes a
new pgcat client that is handed the previous client's server connection. That
is what lets a single psql script check that one client's state does not
reach the next one.

Scenario header lines (SQL comments at the top of the file):
    -- pool: <name>      pgcat pool to run through (repeatable; default psql_tx)
    -- xfail: <reason>   known pgcat bug; the test fails if the bug is fixed
                         so the marker is removed with the fix
    -- xfail-shows: <re> required with xfail: a regex the pgcat-only output
                         (lines just the pgcat run printed, or the hang
                         report) must match for the failure to count as
                         that bug; any other failure fails the test
    -- timeout: <secs>   per-psql-run limit (default 60), for scenarios that hang

Needs psql 18 or newer (\\startpipeline, \\parse, \\bind_named,
\\close_prepared). Set PGCAT_TEST_PSQL to choose the binary. Regenerate .out
files with PGCAT_PSQL_UPDATE=1 and review the diff.
"""

import difflib
import os
import pathlib
import re
import shutil
import subprocess

import psycopg2
import pytest

import utils

PORT = 6450
PG_HOST = "127.0.0.1"
PG_PORT = 5432
DIRECT_DB = "shard0"
SCENARIOS = pathlib.Path(__file__).resolve().parent.parent / "psql"
REPO = SCENARIOS.parent.parent
MIN_PSQL_MAJOR = 18


def pool(name, mode, cache_size=0):
    return f"""
[pools.{name}]
pool_mode = "{mode}"
prepared_statements_cache_size = {cache_size}

[pools.{name}.users.0]
username = "sharding_user"
password = "sharding_user"
pool_size = 1
statement_timeout = 0

[pools.{name}.shards.0]
servers = [["{PG_HOST}", {PG_PORT}, "primary"]]
database = "{DIRECT_DB}"
"""


CONFIG = (
    f"""
[general]
host = "127.0.0.1"
port = {PORT}
admin_username = "admin_user"
admin_password = "admin_pass"
"""
    + pool("psql_tx", "transaction", cache_size=100)
    + pool("psql_tx_small_cache", "transaction", cache_size=2)
    + pool("psql_tx_no_cache", "transaction")
    + pool("psql_session", "session")
)


def psql_major(binary):
    out = subprocess.run([binary, "--version"], capture_output=True, text=True)
    match = re.search(r"(\d+)\.", out.stdout)
    return int(match.group(1)) if match else 0


def find_psql():
    # An explicit choice (as in CI) must work; skipping would hide the suite.
    chosen = os.environ.get("PGCAT_TEST_PSQL")
    if chosen:
        if psql_major(chosen) < MIN_PSQL_MAJOR:
            raise RuntimeError(f"PGCAT_TEST_PSQL={chosen} is not psql {MIN_PSQL_MAJOR}+")
        return chosen
    candidates = sorted(
        (str(p) for p in pathlib.Path("/usr/lib/postgresql").glob("*/bin/psql")),
        key=lambda p: int(pathlib.Path(p).parts[-3]),
        reverse=True,
    )
    candidates.append(shutil.which("psql"))
    for candidate in filter(None, candidates):
        if psql_major(candidate) >= MIN_PSQL_MAJOR:
            return candidate
    return None


PSQL = find_psql()

pytestmark = pytest.mark.skipif(
    PSQL is None, reason=f"needs psql {MIN_PSQL_MAJOR}+ (set PGCAT_TEST_PSQL)"
)


def headers(path):
    pools, xfail, shows, timeout = [], None, None, 60
    for line in path.read_text().splitlines():
        if not line.startswith("--"):
            break
        key, _, value = line[2:].strip().partition(":")
        if key == "pool":
            pools.append(value.strip())
        elif key == "xfail":
            xfail = value.strip()
        elif key == "xfail-shows":
            shows = re.compile(value.strip(), re.M)
        elif key == "timeout":
            timeout = int(value)
    if xfail and shows is None:
        raise ValueError(f"{path.name}: -- xfail: needs -- xfail-shows: <regex>")
    return pools or ["psql_tx"], (xfail, shows), timeout


def cases():
    for path in sorted(SCENARIOS.glob("*.sql")):
        pools, _, _ = headers(path)
        for pool_name in pools:
            yield pytest.param(path, pool_name, id=f"{path.stem}[{pool_name}]")


@pytest.fixture(scope="module")
def database():
    conn = psycopg2.connect(f"postgres://postgres:postgres@{PG_HOST}:{PG_PORT}/{DIRECT_DB}")
    conn.autocommit = True
    cur = conn.cursor()
    # A role sharding_user may SET ROLE to, for the role-leak scenario.
    cur.execute(
        "DO $$ BEGIN "
        "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'psql_scenario_role') "
        "THEN CREATE ROLE psql_scenario_role NOLOGIN; END IF; END $$"
    )
    cur.execute("GRANT psql_scenario_role TO sharding_user")
    conn.close()


# One pgcat per scenario: a scenario showing a leak leaves the leaked state on
# the pooled server connection, and it must not reach the next scenario.
@pytest.fixture
def pgcat(database):
    process = utils.PgcatProcess(CONFIG, PORT).start()
    yield process
    process.stop()


class PsqlHung(Exception):
    pass


def pgcat_only_lines(problem):
    """The part of a failure that is pgcat's: lines only the pgcat run
    printed, or the hang report."""
    if not problem.startswith("psql transcript differs"):
        return problem
    return "".join(
        line[1:] for line in problem.splitlines(True)
        if line.startswith("+") and not line.startswith("+++")
    )


def run_psql(path, port, db):
    """Run a scenario and return its transcript and psql's exit code."""
    timeout = headers(path)[2]
    env = dict(
        os.environ,
        PGPASSWORD="sharding_user",
        PGAPPNAME="psql_scenario",
        PGCLIENTENCODING="UTF8",
        LC_ALL="C",
    )
    # Paths relative to the repo keep psql's "psql:<file>:<line>:" prefixes
    # identical between runs and machines.
    command = [
        PSQL, "-X", "-q", "--no-password",
        "-v", "ON_ERROR_STOP=0", "-v", "VERBOSITY=default",
        "-h", PG_HOST, "-p", str(port), "-U", "sharding_user", "-d", db,
        "-f", str(path.relative_to(REPO)),
    ]
    try:
        result = subprocess.run(
            command, cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as hung:
        stdout, stderr = (
            out.decode(errors="replace") if isinstance(out, bytes) else out or ""
            for out in (hung.stdout, hung.stderr)
        )
        # What psql printed before it stuck is what shows how far it got.
        raise PsqlHung(
            f"psql did not finish within {timeout}s on port {port}:\n"
            f"{stdout}--- stderr ---\n{stderr}"
        )
    # Kept apart: merging the pipes would order them by buffering, not by statement.
    return result.stdout + "\n--- stderr ---\n" + result.stderr, result.returncode


def mismatch(expected, actual, expected_label):
    if expected == actual:
        return None
    return "psql transcript differs:\n" + "".join(
        difflib.unified_diff(
            expected.splitlines(True), actual.splitlines(True),
            fromfile=expected_label, tofile="through pgcat",
        )
    )


@pytest.mark.parametrize("path,pool_name", list(cases()))
def test_scenario(pgcat, path, pool_name):
    _, (known_bug, shows), _ = headers(path)
    golden = path.with_suffix(".out")

    # The reference side is outside the known-bug scope: if Postgres itself
    # can't run the scenario, that is a broken test, never an expected failure.
    if golden.exists():
        expected, label = golden.read_text(), str(golden.relative_to(REPO))
    else:
        # Scenarios clean up after themselves, so both runs start from the same state.
        expected, code = run_psql(path, PG_PORT, DIRECT_DB)
        assert code == 0, f"psql exited {code} directly against Postgres:\n{expected}"
        label = "directly against Postgres"

    try:
        actual, code = run_psql(path, PORT, pool_name)
    except PsqlHung as hung:
        problem = str(hung)
    else:
        if golden.exists() and os.environ.get("PGCAT_PSQL_UPDATE") == "1":
            golden.write_text(actual)
            expected = actual
        problem = mismatch(expected, actual, label)
        if problem is None and code != 0:
            problem = f"psql exited {code} through pgcat"

    if known_bug is None:
        if problem:
            pytest.fail(problem, pytrace=False)
    elif problem and shows.search(pgcat_only_lines(problem)):
        pytest.xfail(known_bug)
    elif problem:
        pytest.fail(
            f"fails, but not with the known bug (-- xfail-shows: {shows.pattern}):\n{problem}",
            pytrace=False,
        )
    else:
        pytest.fail(
            f"matches now; remove the -- xfail: header from {path.name} with the fix",
            pytrace=False,
        )
