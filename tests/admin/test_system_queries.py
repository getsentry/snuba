from typing import Sequence

import pytest

from snuba import settings
from snuba.admin.auth_roles import ROLES, Role
from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.system_queries import (
    UnauthorizedForSudo,
    is_query_alter,
    is_query_select,
    is_system_command,
    run_system_query_on_host_with_sql,
    validate_system_query,
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
    ],
)
def test_valid_system_query(sql_query: str) -> None:
    validate_system_query(sql_query)


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
    ],
)
def test_invalid_system_query(sql_query: str) -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_system_query(sql_query)


select_sql = """
SELECT
    table,
    query,
    format,
    query_id,
    bytes,
    flush_time,
    flush_query_id
FROM
    system.asynchronous_insert_log
WHERE
    status = 'Ok'
    AND database = 'default'
    AND flush_time > now() - toIntervalMinute(10)
ORDER BY table, flush_time
"""


def test_is_query_select() -> None:
    assert is_query_select(select_sql) == True


@pytest.mark.parametrize(
    "sudo_query, expected",
    [
        ("SYSSSSSSSTEM DO SOMETHING", False),
        ("SYSTEM STOP MERGES", True),
        ("system STOP MerGes", True),
        ("system SHUTDOWN", False),
        ("system KILL", False),
        ("ALTER TABLE eap_spans_local_merge DROP PARTITION '1970-01-01'", True),
        ("CREATE TABLE eap_spans_local_merge (all my fieds)", True),
    ],
)
def test_sudo_queries(sudo_query: str, expected: bool) -> None:
    assert (is_system_command(sudo_query) or is_query_alter(sudo_query)) == expected
    with pytest.raises(InvalidCustomQuery):
        validate_system_query(sudo_query)


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
