from typing import Sequence

import pytest

from snuba import settings
from snuba.admin.auth_roles import ROLES, Role
from snuba.admin.clickhouse.system_queries import (
    UnauthorizedForSudo,
    is_valid_system_query,
    run_system_query_on_host_with_sql,
    validate_query,
)
from snuba.admin.user import AdminUser


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
@pytest.mark.clickhouse_db
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
@pytest.mark.clickhouse_db
def test_invalid_system_query(sql_query: str) -> None:
    with pytest.raises(Exception):
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
        ("SYSTEM STOP TTL MERGES ON CLUSTER 'snuba-spans'", True),
        ("KILL MUTATION WHERE mutation_id='0000000000'", True),
        ("system STOP MerGes", True),
        ("system SHUTDOWN", False),
        ("system KILL", False),
        ("ALTER TABLE eap_spans_local_merge DROP PARTITION '1970-01-01'", True),
        ("CREATE TABLE eap_spans_local_merge (all my fieds)", True),
        ("OPTIMIZE TABLE eap_spans_local", True),
        ("optimize table eap_spans_local", True),
        ("optimize   TABLE eap_spans_local", True),
        (
            "SYSTEM DROP REPLICA 'snuba-events-analytics-platform-2-2' FROM ZKPATH '/clickhouse/tables/events_analytics_platform/2/default/eap_spans_2_local'",
            True,
        ),
    ],
)
@pytest.mark.clickhouse_db
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
        with pytest.raises(Exception):
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
@pytest.mark.clickhouse_db
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
        with pytest.raises(Exception):
            run_query()
    else:
        run_query()


@pytest.mark.parametrize(
    "sql_query, sudo_mode",
    [
        ("SELECT * FROM system.clusters;", True),
        ("SELECT * FROM system.clusters;", False),
    ],
)
@pytest.mark.clickhouse_db
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

        try:
            is_valid_system_query(
                settings.CLUSTERS[0]["host"],
                int(settings.CLUSTERS[0]["port"]),
                "errors",
                sql_query,
                False,
                sudo_mode,
            )
        except Exception:
            pass  # We don't care if validation fails, we just want to check the query

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
