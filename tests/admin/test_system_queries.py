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
