import ast
import contextlib
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.admin.auth_roles import ROLES, Role
from snuba.admin.clickhouse.common import InvalidCustomQuery, InvalidNodeError
from snuba.admin.clickhouse.system_queries import (
    UnauthorizedForSudo,
    is_valid_system_query,
    run_system_query_on_host_with_sql,
    validate_query,
)
from snuba.admin.user import AdminUser
from snuba.clusters.cluster import ClickhouseClientSettings


@pytest.mark.parametrize(
    "sql_query",
    [
        "SELECT * FROM system.clusters;",  # trailing semicolon
        "SELECT * FROM system.clusters",  # no trailing semicolon
        "select * from system.clusters;",  # lowercase
        "SELECT  *    FROM   \nsystem.clusters;",  # whitespace
        "SELECT cluster, is_local FROM system.clusters",  # select by col name
        "select sum(bytes) from system.parts group by table;",  # function in select clause
        "SELECT * FROM system.clusters WHERE cluster == 'my_cluster'",  # where clause
        "SELECT * FROM system.clusters WHERE toInt32(shard_num) == 1",  # where clause with fn
        "SELECT * FROM system.clusters LIMIT 100",  # limit
        "SELECT empty('str') FROM system.clusters LIMIT 100",  # literal str params
        "SELECT * FROM system.query_log WHERE event_time > toDateTime('2023-07-05 14:24:00') AND event_time < toDateTime('2023-07-05T14:34:00')",  # datetimes
        """SELECT
            count() as nb_query,
            user,
            query,
            sum(memory_usage) AS memory,
            normalized_query_hash
        FROM
            system.query_log
        WHERE
            (event_time >= (now() - toIntervalDay(1)))
            AND query_kind = 'Select'
            AND type = 'QueryFinish'
            and user != 'monitoring-internal'
        GROUP BY
            normalized_query_hash,
            query,
            user
        ORDER BY
            memory DESC
        """,
        "SELECT hostname(), avg(query_duration_ms) FROM clusterAllReplicas('default', system.query_log) GROUP BY hostname()",
        "SELECT count() FROM merge('system', '.*settings')",
    ],
)
@pytest.mark.events_db
def test_is_valid_system_query(sql_query: str) -> None:
    assert is_valid_system_query(
        settings.CLUSTERS[0]["host"], int(settings.CLUSTERS[0]["port"]), "errors", sql_query, False
    )


@pytest.mark.parametrize(
    "sql_query",
    [
        "SHOW TABLES;",  # non select statement
        "SELECT * FROM my_table;",  # not allowed table
        "SELECT * from system.metrics"  # system table not on allowed list
        "with sum(bytes) as s select s from system.parts group by table;",  # sorry not allowed WITH
        "SELECT 1; SELECT 2;"  # no multiple statements
        "SELECT * FROM system.clusters c INNER JOIN my_table m ON c.cluster == m.something",  # no join
        "SELECT * from system.as1",  # invalid system table format
        """SELECT
            count() as nb_query,
            user,
            query,
            sum(memory_usage) AS memory,
            normalized_query_hash
        FROM
            clusterAllReplicas(default, system.query_log)
        WHERE
            (event_time >= (now() - toIntervalDay(1)))
            AND query_kind = 'Select'
            AND type = 'QueryFinish'
            and user != 'monitoring-internal'
        GROUP BY
            normalized_query_hash,
            query,
            user
        ORDER BY
            memory DESC;
        """,
    ],
)
@pytest.mark.events_db
def test_invalid_system_query(sql_query: str) -> None:
    with pytest.raises(InvalidCustomQuery):
        is_valid_system_query(
            settings.CLUSTERS[0]["host"],
            int(settings.CLUSTERS[0]["port"]),
            "errors",
            sql_query,
            False,
        )


@pytest.mark.parametrize(
    "sudo_query, expected",
    [
        ("SYSSSSSSSTEM DO SOMETHING", False),
        ("SYSTEM STOP MERGES", True),
        ("SYSTEM STOP TTL MERGES", True),
        ("KILL MUTATION WHERE mutation_id='0000000000'", True),
        ("system STOP MerGes", True),
        ("system SHUTDOWN", False),
        ("system KILL", False),
        ("ALTER TABLE eap_spans_local_merge DROP PARTITION '1970-01-01'", True),
        ("CREATE TABLE eap_spans_local_merge (all my fieds)", True),
        ("OPTIMIZE TABLE eap_spans_local", True),
        ("optimize table eap_spans_local", True),
        ("optimize   TABLE eap_spans_local", True),
        ("DROP TABLE eap_spans_local", True),
        ("drop table eap_spans_local", True),
        ("DROP TABLE IF EXISTS eap_spans_local", True),
        (
            "DROP TABLE IF EXISTS default.eap_items_1_dist ON CLUSTER 'snuba-events-analytics-platform' SYNC;",
            True,
        ),
        (
            "SYSTEM DROP REPLICA 'snuba-events-analytics-platform-2-2' FROM ZKPATH '/clickhouse/tables/events_analytics_platform/2/default/eap_spans_2_local'",
            True,
        ),
    ],
)
@pytest.mark.events_db
def test_sudo_queries(sudo_query: str, expected: bool) -> None:
    if expected:
        validate_query(
            settings.CLUSTERS[0]["host"],
            int(settings.CLUSTERS[0]["port"]),
            "errors",
            sudo_query,
            True,
            False,
        )  # Should no-op
    else:
        with pytest.raises(InvalidCustomQuery):
            validate_query(
                settings.CLUSTERS[0]["host"],
                int(settings.CLUSTERS[0]["port"]),
                "errors",
                sudo_query,
                True,
                False,
            )


@pytest.mark.parametrize(
    "query, roles, sudo_mode, expect_authorized, expect_valid",
    [
        pytest.param(
            "SYSTEM START MERGES",
            [ROLES["ClickhouseAdmin"], ROLES["ProductTools"]],
            True,
            True,
            True,
            id="Admin user",
        ),
        pytest.param(
            "SYSTEM START MERGES",
            [ROLES["ClickhouseAdmin"], ROLES["ProductTools"]],
            False,
            True,
            False,
            id="Admin not using sudo",
        ),
        pytest.param(
            "SYSTEM START MERGES",
            [ROLES["ProductTools"]],
            True,
            False,
            True,
            id="Not admin trying to use sudo",
        ),
        pytest.param(
            "SYSTEM START MERGES",
            [ROLES["ProductTools"]],
            False,
            True,
            False,
            id="Not admin not trying sudo. Query invalid",
        ),
    ],
)
@pytest.mark.events_db
def test_run_sudo_queries(
    query: str,
    roles: Sequence[Role],
    sudo_mode: bool,
    expect_authorized: bool,
    expect_valid: bool,
) -> None:
    def run_query() -> None:
        run_system_query_on_host_with_sql(
            settings.CLUSTERS[0]["host"],
            int(settings.CLUSTERS[0]["port"]),
            "errors",
            query,
            sudo_mode,
            False,
            AdminUser(
                "me@myself.org",
                "me@myself.org",
                roles,
            ),
        )

    if not expect_authorized:
        with pytest.raises(UnauthorizedForSudo):
            run_query()
    elif not expect_valid:
        with pytest.raises(InvalidCustomQuery):
            run_query()
    else:
        run_query()


@pytest.mark.parametrize(
    "sudo_mode, expected_helper",
    [
        pytest.param(
            False,
            "get_ro_clusterless_node_connection",
            id="Non-sudo clusterless uses readonly credentials",
        ),
        pytest.param(
            True,
            "get_clusterless_node_connection",
            id="Sudo clusterless uses cluster admin credentials",
        ),
    ],
)
def test_clusterless_uses_readonly_for_non_sudo(sudo_mode: bool, expected_helper: str) -> None:
    """
    Non-sudo clusterless system queries must connect with the global readonly
    user. Without this, the default NOOP auth provider would let anonymous
    users run queries against ClickHouse with the full cluster admin
    credentials, leaking sensitive data via system tables.
    """
    from unittest.mock import patch

    from snuba.admin.clickhouse import system_queries

    mock_result = type("MockResult", (), {"results": []})()
    forbidden = (
        "get_clusterless_node_connection"
        if expected_helper == "get_ro_clusterless_node_connection"
        else "get_ro_clusterless_node_connection"
    )

    with (
        patch.object(system_queries, expected_helper) as mock_used,
        patch.object(system_queries, forbidden) as mock_forbidden,
    ):
        mock_used.return_value.execute.return_value = mock_result

        system_queries._run_sql_query_on_host(
            "host",
            9000,
            "errors",
            "SELECT * FROM system.clusters",
            sudo_mode,
            True,
        )

        assert mock_used.called, f"Expected {expected_helper} to be used"
        assert not mock_forbidden.called, f"{forbidden} must not be used in this mode"


@pytest.mark.parametrize(
    "helper_name, client_settings",
    [
        pytest.param(
            "get_clusterless_node_connection",
            ClickhouseClientSettings.QUERY,
            id="sudo clusterless helper",
        ),
        pytest.param(
            "get_ro_clusterless_node_connection",
            ClickhouseClientSettings.QUERY,
            id="readonly clusterless helper",
        ),
    ],
)
def test_clusterless_rejects_unvalidated_host(
    helper_name: str, client_settings: ClickhouseClientSettings
) -> None:
    """
    Regression for EAP-488: the clusterless helpers used to construct a
    ClickhousePool against any attacker-supplied host/port, which leaked the
    configured ClickHouse user/password in the first hello packet of the
    native protocol. Both helpers must now call _validate_node before
    constructing the pool, so an invalid host produces InvalidNodeError and
    no credentials ever leave the process.
    """
    from snuba.admin.clickhouse import common

    helper = getattr(common, helper_name)

    with (
        patch.object(
            common,
            "_validate_node",
            side_effect=InvalidNodeError("host not in cluster"),
        ) as mock_validate,
        patch.object(common, "ClickhousePool") as mock_pool,
    ):
        # Clear any cached connection for this storage so the cache lookup
        # can't short-circuit validation.
        for key in [k for k in common.NODE_CONNECTIONS if k.startswith("errors-")]:
            del common.NODE_CONNECTIONS[key]

        with pytest.raises(InvalidNodeError):
            helper("attacker.example.com", 9009, "errors", client_settings)

        assert mock_validate.called, "_validate_node must run before pool construction"
        assert not mock_pool.called, (
            "ClickhousePool must not be constructed for an unvalidated host — "
            "doing so would transmit ClickHouse credentials to the attacker"
        )


@pytest.mark.parametrize(
    "sql_query, sudo_mode",
    [
        ("SELECT * FROM system.clusters;", True),
        ("SELECT * FROM system.clusters;", False),
    ],
)
@pytest.mark.events_db
def test_sudo_mode_skips_experimental_analyzer(sql_query: str, sudo_mode: bool) -> None:
    """
    Test that when sudo_mode=True, the experimental analyzer setting is not
    appended to the EXPLAIN QUERY TREE command.
    """
    from unittest.mock import patch

    with patch("snuba.admin.clickhouse.system_queries._run_sql_query_on_host") as mock_run:
        # Mock the response to simulate successful validation
        mock_result = type("MockResult", (), {"results": []})()
        mock_run.return_value = mock_result

        # We don't care if validation fails, we just want to check the query
        with contextlib.suppress(Exception):
            is_valid_system_query(
                settings.CLUSTERS[0]["host"],
                int(settings.CLUSTERS[0]["port"]),
                "errors",
                sql_query,
                False,
                sudo_mode,
            )

        # Check that the EXPLAIN QUERY TREE was called
        calls = [call for call in mock_run.call_args_list if "EXPLAIN QUERY TREE" in str(call)]
        assert len(calls) > 0, "Expected EXPLAIN QUERY TREE to be called"

        # Get the explain query from the call - it's the 4th positional argument (index 3)
        # call signature: _run_sql_query_on_host(host, port, storage, sql, sudo, clusterless)
        explain_query = calls[0][0][3]  # Fourth argument is the SQL query

        if sudo_mode:
            # Should NOT contain the experimental analyzer setting
            assert "allow_experimental_analyzer" not in explain_query, (
                f"Sudo mode should not use experimental analyzer, but got: {explain_query}"
            )
        else:
            # Should contain the experimental analyzer setting
            assert "allow_experimental_analyzer" in explain_query, (
                f"Non-sudo mode should use experimental analyzer, but got: {explain_query}"
            )


def _find_clickhouse_pool_calls(tree: ast.AST) -> list[tuple[ast.Call, list[str]]]:
    """
    Walks an AST and returns every `ClickhousePool(...)` call site, paired
    with the chain of enclosing function names (outermost first) so the
    regression guard below can assert *where* construction happens.
    """
    results: list[tuple[ast.Call, list[str]]] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.scope: list[str] = []

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if name == "ClickhousePool":
                results.append((node, list(self.scope)))
            self.generic_visit(node)

    Visitor().visit(tree)
    return results


def test_no_direct_clickhouse_pool_construction_in_admin() -> None:
    """
    Defense-in-depth for EAP-488: ClickhousePool ships the configured
    user/password in the first hello packet of the native protocol, so any
    admin code path that constructs one against a caller-supplied host
    leaks credentials to whatever listener answers. `_build_validated_pool`
    in snuba/admin/clickhouse/common.py is the single chokepoint that runs
    `_validate_node` first — every other admin module must go through it.

    This test enforces that structural invariant by AST-walking every
    snuba/admin/**/*.py file:

    * common.py may only construct ClickhousePool from inside
      `_build_validated_pool`.
    * No other admin module may construct ClickhousePool at all.

    If this fails, a new caller has likely re-introduced the vulnerability.
    """
    admin_root = Path(__file__).resolve().parents[2] / "snuba" / "admin"
    assert admin_root.is_dir(), f"expected admin root at {admin_root}"

    common_path = admin_root / "clickhouse" / "common.py"
    offenders: list[str] = []

    for py_file in sorted(admin_root.rglob("*.py")):
        tree = ast.parse(py_file.read_text(), filename=str(py_file))
        for call, scope in _find_clickhouse_pool_calls(tree):
            rel = py_file.relative_to(admin_root.parent.parent)
            location = f"{rel}:{call.lineno}"
            if py_file == common_path:
                if scope != ["_build_validated_pool"]:
                    offenders.append(
                        f"{location} constructs ClickhousePool inside "
                        f"{'.'.join(scope) or '<module>'} — must be inside "
                        "_build_validated_pool so _validate_node runs first."
                    )
            else:
                offenders.append(
                    f"{location} constructs ClickhousePool directly — call "
                    "_build_validated_pool (or a helper that wraps it) so "
                    "_validate_node guards the host before credentials are sent."
                )

    assert not offenders, "\n".join(offenders)
