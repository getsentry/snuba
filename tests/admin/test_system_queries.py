import pytest

from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.system_queries import validate_system_query


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
