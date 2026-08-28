import ast
import contextlib
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import MagicMock, patch

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
from snuba.clickhouse.errors import ClickhouseError
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
    # These queries are rejected either by Snuba's own validation
    # (InvalidCustomQuery) or by ClickHouse when EXPLAIN runs on a
    # malformed/non-SELECT statement (ClickhouseError).
    with pytest.raises((InvalidCustomQuery, ClickhouseError)):
        is_valid_system_query(
            settings.CLUSTERS[0]["host"],
            int(settings.CLUSTERS[0]["port"]),
            "errors",
            sql_query,
            False,
        )


@pytest.mark.events_db
def test_merge_table_function_is_rejected() -> None:
    # merge() is valid ClickHouse SQL. EXPLAIN succeeds, then
    # is_query_using_only_system_tables returns False because the analyzer
    # does not resolve merge() to system.* tables.
    assert not is_valid_system_query(
        settings.CLUSTERS[0]["host"],
        int(settings.CLUSTERS[0]["port"]),
        "errors",
        "SELECT * FROM merge('default', 'errors.*')",
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
        with pytest.raises((InvalidCustomQuery, ClickhouseError)):
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
        with pytest.raises((InvalidCustomQuery, ClickhouseError)):
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
def test_clusterless_allows_hosts_outside_cluster_topology(
    helper_name: str, client_settings: ClickhouseClientSettings
) -> None:
    """
    Clusterless helpers intentionally skip host/topology validation so ops
    tools (copy-tables) can target brand-new nodes that are not yet members of
    the storage cluster. `_validate_node` must not gate these helpers.
    """
    from snuba.admin.clickhouse import common

    helper = getattr(common, helper_name)

    with (
        patch.object(
            common,
            "_validate_node",
            side_effect=InvalidNodeError("host not in cluster"),
        ) as mock_validate,
        patch.object(common, "build_pool") as mock_pool,
    ):
        helper("new-node.example.com", 9009, "errors", client_settings)

        assert not mock_validate.called, (
            "clusterless helpers must not call _validate_node; brand-new nodes "
            "are expected to sit outside known cluster topology"
        )
        assert mock_pool.called, "clusterless helpers must still acquire a pool"


@pytest.mark.parametrize(
    "helper_name",
    [
        "get_ro_node_connection",
        "get_sudo_node_connection",
        "get_clusterless_node_connection",
        "get_ro_clusterless_node_connection",
    ],
)
def test_by_host_connection_uses_default_http_port(helper_name: str) -> None:
    """
    By-host helpers target a replica. Replicas listen on 8123, not the
    cluster Envoy port from get_port().
    """
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import (
        DEFAULT_CLICKHOUSE_HTTP_PORT,
        ClickhouseCluster,
    )

    helper = getattr(common, helper_name)

    # A port that is deliberately not the well-known default, so the assertions
    # below distinguish "used the default" from "used the cluster's port" even
    # if the test cluster happens to be configured with the default port.
    sentinel_cluster_http_port = 65432

    with (
        patch.object(common, "_validate_node"),  # treat the host as valid
        patch.object(
            ClickhouseCluster,
            "get_port",
            return_value=sentinel_cluster_http_port,
        ),
        patch.object(common, "build_pool") as mock_pool,
    ):
        helper(
            "specific-node.example.com",
            9000,
            "errors",
            ClickhouseClientSettings.QUERY,
        )

    assert mock_pool.called, "expected a pool to be acquired for a valid host"
    node = mock_pool.call_args.args[1]
    assert node.port == DEFAULT_CLICKHOUSE_HTTP_PORT
    assert node.port != sentinel_cluster_http_port, (
        "by-host connections must not use the cluster's configured port"
    )


def test_is_valid_node_accepts_query_endpoint_when_topology_fails() -> None:
    """Configured query endpoint must stay valid if topology discovery fails."""
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode

    cluster = MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 9000)
    cluster.get_query_node.return_value = query_node
    cluster.get_port.return_value = 8123
    cluster.get_local_nodes.side_effect = Exception("topology down")
    cluster.get_distributed_nodes.side_effect = Exception("topology down")

    assert common.is_valid_node("query", 8123, cluster) is True


def test_is_valid_node_rejects_unknown_host_when_topology_fails() -> None:
    from snuba.admin.clickhouse import common
    from snuba.admin.clickhouse.common import InvalidNodeError
    from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode

    cluster = MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("query", 9000)
    cluster.get_port.return_value = 8123
    cluster.get_local_nodes.side_effect = Exception("topology down")
    cluster.get_distributed_nodes.side_effect = Exception("topology down")

    with pytest.raises(InvalidNodeError):
        common.is_valid_node("attacker.example.com", 8123, cluster)


def test_is_valid_node_accepts_discovered_storage_node() -> None:
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode

    cluster = MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 9000)
    storage_node = ClickhouseNode("storage", 8123)
    cluster.get_query_node.return_value = query_node
    cluster.get_port.return_value = 8123
    cluster.get_local_nodes.return_value = [storage_node]
    cluster.get_distributed_nodes.return_value = []

    assert common.is_valid_node("storage", 8123, cluster) is True


def test_is_valid_node_uses_known_nodes_without_rediscovery() -> None:
    """Callers with a topology snapshot must not re-query discovery."""
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode

    cluster = MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 9000)
    storage_node = ClickhouseNode("storage", 8123)
    cluster.get_query_node.return_value = query_node
    cluster.get_port.return_value = 8123
    cluster.get_local_nodes.side_effect = Exception("should not rediscover")
    cluster.get_distributed_nodes.side_effect = Exception("should not rediscover")

    assert (
        common.is_valid_node(
            "storage",
            8123,
            cluster,
            known_nodes=[query_node, storage_node],
        )
        is True
    )
    cluster.get_local_nodes.assert_not_called()
    cluster.get_distributed_nodes.assert_not_called()


def test_ro_cluster_node_connection_uses_validated_readonly_pool() -> None:
    """Cluster-scoped admin lookups must stay on the validated readonly path."""
    from unittest.mock import MagicMock

    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode

    cluster = MagicMock(spec=ClickhouseCluster)
    cluster.get_database.return_value = "default"
    cluster.get_secure.return_value = False
    cluster.get_ca_certs.return_value = None
    cluster.get_verify.return_value = False
    cluster.get_port.return_value = 8123
    cluster.get_query_node.return_value = ClickhouseNode("query", 8123)
    node = ClickhouseNode("storage", 8123)
    known_nodes = [cluster.get_query_node.return_value, node]

    with (
        patch.object(common, "_validate_node") as mock_validate,
        patch.object(settings, "CLICKHOUSE_READONLY_USER", "ro_user"),
        patch.object(settings, "CLICKHOUSE_READONLY_PASSWORD", "ro_pass"),
        patch.object(common, "build_pool") as mock_pool,
    ):
        common.get_ro_cluster_node_connection(
            cluster, node, ClickhouseClientSettings.QUERY, known_nodes=known_nodes
        )

    mock_validate.assert_called_once_with("storage", 8123, cluster, None, known_nodes=known_nodes)
    assert mock_pool.called
    assert mock_pool.call_args.args[2] == "ro_user"
    assert mock_pool.call_args.args[3] == "ro_pass"


def test_query_node_connection_uses_cluster_http_port() -> None:
    """
    Counterpart to test_by_host_connection_uses_default_http_port: the
    query-node helper (get_ro_query_node_connection — used by the tracing,
    querylog and cardinality tools) connects to the cluster's *configured query
    endpoint*, the same host the normal read path reaches on
    cluster.get_port(). That endpoint may be a load balancer on a
    non-default HTTP port, so this path must keep using cluster.get_port()
    rather than the by-host default — otherwise the HTTP driver would send those
    tools to the wrong port.
    """
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster

    # A port that is deliberately not the well-known default, so the assertion
    # below proves the query-node path uses the cluster's configured port and
    # not the by-host default.
    sentinel_cluster_http_port = 65432

    with (
        patch.object(common, "_validate_node"),  # treat the host as valid
        patch.object(
            ClickhouseCluster,
            "get_port",
            return_value=sentinel_cluster_http_port,
        ),
        patch.object(common, "build_pool") as mock_pool,
    ):
        common.get_ro_query_node_connection("errors", ClickhouseClientSettings.QUERY)

    assert mock_pool.called, "expected a pool to be acquired for the query node"
    node = mock_pool.call_args.args[1]
    assert node.port == sentinel_cluster_http_port, (
        "the query-node connection must use the cluster's configured port"
    )


def test_ro_profiles_use_matching_credentials() -> None:
    """QUERY uses the readonly user; CARDINALITY_ANALYZER uses the trace user."""
    from snuba.admin.clickhouse import common

    with (
        patch.object(common, "_validate_node"),
        patch.object(settings, "CLICKHOUSE_READONLY_USER", "ro_user"),
        patch.object(settings, "CLICKHOUSE_TRACE_USER", "trace_user"),
        patch.object(common, "build_pool") as mock_pool,
    ):
        common.get_ro_query_node_connection("errors", ClickhouseClientSettings.QUERY)
        common.get_ro_query_node_connection("errors", ClickhouseClientSettings.CARDINALITY_ANALYZER)

    profiles = [call.args[0] for call in mock_pool.call_args_list]
    usernames = [call.args[2] for call in mock_pool.call_args_list]
    assert profiles == [
        ClickhouseClientSettings.QUERY,
        ClickhouseClientSettings.CARDINALITY_ANALYZER,
    ]
    assert usernames == ["ro_user", "trace_user"]


def test_validated_pool_propagates_cluster_tls_settings() -> None:
    """Admin pools must inherit the cluster TLS config, not hardcode insecure."""
    from snuba.admin.clickhouse import common
    from snuba.clusters.cluster import ClickhouseCluster

    with (
        patch.object(common, "_validate_node"),
        patch.object(ClickhouseCluster, "get_secure", return_value=True),
        patch.object(ClickhouseCluster, "get_ca_certs", return_value="/etc/ssl/certs/ca.pem"),
        patch.object(ClickhouseCluster, "get_verify", return_value=True),
        patch.object(common, "build_pool") as mock_pool,
    ):
        common.get_ro_query_node_connection("errors", ClickhouseClientSettings.QUERY)

    assert mock_pool.called
    kwargs = mock_pool.call_args.kwargs
    assert kwargs["secure"] is True
    assert kwargs["ca_certs"] == "/etc/ssl/certs/ca.pem"
    assert kwargs["verify"] is True


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

    with patch("snuba.admin.clickhouse.system_queries._run_explain_on_host") as mock_run:
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
        # call signature: _run_explain_on_host(host, port, storage, sql, clusterless)
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


# Names that acquire a ClickhousePool (and therefore send credentials to a
# node). Outside common.py these must go through _build_pool helpers.
_POOL_ACQUISITION_NAMES = frozenset(
    {
        "build_pool",
        "get_node_connection",
        "ClickhouseConnectPool",
    }
)


def _find_clickhouse_pool_calls(tree: ast.AST) -> list[tuple[ast.Call, list[str]]]:
    """
    Walks an AST and returns every pool-acquisition call site (cache accessor
    or direct pool construction), paired with the chain of enclosing function
    names (outermost first) so the regression guard below can assert *where*
    acquisition happens.
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
            if name in _POOL_ACQUISITION_NAMES:
                results.append((node, list(self.scope)))
            self.generic_visit(node)

    Visitor().visit(tree)
    return results


def test_no_direct_clickhouse_pool_construction_in_admin() -> None:
    """
    Defense-in-depth for EAP-488: a ClickhousePool ships the configured
    user/password to the node (the driver's first hello packet, or
    the HTTP auth header), so any admin code path that acquires one against a
    caller-supplied host can leak credentials to whatever listener answers.

    `snuba/admin/clickhouse/common.py` is the only place that may acquire a
    pool, and only from `_build_pool` / `_build_validated_pool`:

    * `_build_validated_pool` runs `_validate_node` first for normal helpers.
    * `_build_pool` is the shared constructor; clusterless helpers call it
      directly so brand-new nodes can be targeted before cluster membership.

    "Acquiring" a pool means either constructing one of the pool
    implementations directly or fetching one from the shared connection cache
    via `get_node_connection`. This test enforces that structural invariant by
    AST-walking every snuba/admin/**/*.py file.

    If this fails, a new caller has likely re-introduced a direct pool path.
    """
    admin_root = Path(__file__).resolve().parents[2] / "snuba" / "admin"
    assert admin_root.is_dir(), f"expected admin root at {admin_root}"

    common_path = admin_root / "clickhouse" / "common.py"
    allowed_common_scopes = {"_build_pool", "_build_validated_pool"}
    offenders: list[str] = []

    for py_file in sorted(admin_root.rglob("*.py")):
        tree = ast.parse(py_file.read_text(), filename=str(py_file))
        for call, scope in _find_clickhouse_pool_calls(tree):
            rel = py_file.relative_to(admin_root.parent.parent)
            location = f"{rel}:{call.lineno}"
            if py_file == common_path:
                if not scope or scope[-1] not in allowed_common_scopes:
                    offenders.append(
                        f"{location} acquires a ClickhousePool inside "
                        f"{'.'.join(scope) or '<module>'} — must be inside "
                        "_build_pool or _build_validated_pool."
                    )
            else:
                offenders.append(
                    f"{location} acquires a ClickhousePool directly — call "
                    "_build_validated_pool / _build_pool (or a helper that "
                    "wraps them) instead of constructing a pool here."
                )

    assert not offenders, "\n".join(offenders)
