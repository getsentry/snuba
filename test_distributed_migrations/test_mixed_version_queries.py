"""
Run errors queries the way Snuba sends them: through the query node's
Distributed tables to the storage shards.

On its own this is a smoke test of the multi-node topology. Its real purpose is
the clickhouse-mixed-versions CI job, which puts the query node and the storage
shards on different ClickHouse versions to simulate a cluster in the middle of a
rolling upgrade. INC-2695 was this situation: 26.8 query nodes and 25.8 storage
nodes disagreed on how dotted Nested column names are quoted in the block
headers they exchange, so every query touching `exception_stacks.*` or `tags.*`
failed with NOT_FOUND_COLUMN_IN_BLOCK. See
https://github.com/ClickHouse/ClickHouse/issues/113250.

The queries mirror shapes Snuba generates, and each one checks its result as
well as checking that it ran: a mixed cluster could in principle answer wrongly
instead of failing.
"""

import os
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.pool import ClickhousePool
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web.db_query import execute_query

PROJECT_ID = 4500
UNHANDLED_GROUP = 101  # newest event is unhandled
HANDLED_GROUP = 102  # every event is handled
EVENTS_PER_GROUP = 6

# Fixed IDs so the cityHash64(event_id) sharding is deterministic. These put
# rows from both groups on both storage shards (or the one shard, in the
# single-shard layout).
EVENT_IDS = [
    str(uuid.uuid5(uuid.NAMESPACE_URL, f"snuba-mixed-version-test-{i}"))
    for i in range(2 * EVENTS_PER_GROUP)
]

# Where every query below filters, like Snuba's generated SQL does.
CONDITIONS = (
    f"project_id IN tuple({PROJECT_ID}) AND deleted = 0 "
    "AND timestamp >= now() - toIntervalDay(1) AND timestamp < now() + toIntervalHour(1)"
)


def _connection() -> ClickhousePool:
    return (
        get_storage(StorageKey.ERRORS)
        .get_cluster()
        .get_query_connection(ClickhouseClientSettings.QUERY)
    )


def _is_mixed_cluster() -> bool:
    """True when EXPECTED_CLICKHOUSE_VERSIONS lists more than one version."""
    expected = os.environ.get("EXPECTED_CLICKHOUSE_VERSIONS", "")
    return len({pair.split("=")[1] for pair in expected.split(",") if pair}) > 1


def _normalize(rows: Sequence[Sequence[Any]]) -> list[tuple[Any, ...]]:
    # The HTTP interface returns 64-bit integers as JSON strings.
    return [tuple(int(v) if isinstance(v, str) and v.isdigit() else v for v in row) for row in rows]


def test_cluster_versions() -> None:
    """
    Print each node's version. If EXPECTED_CLICKHOUSE_VERSIONS is set
    (e.g. "clickhouse-query=26.8,clickhouse-02=25.8"), also assert it, so a
    matrix cell that didn't actually get the images it asked for can't pass
    by testing a single-version cluster.
    """
    conn = _connection()
    versions = {"clickhouse-query": str(conn.execute("SELECT version()").results[0][0])}
    for host, version in conn.execute(
        "SELECT hostName(), version() FROM clusterAllReplicas('storage_cluster', system.one)"
    ).results:
        versions[str(host).split(".")[0]] = str(version)
    print(f"ClickHouse versions: {versions}")

    expected = os.environ.get("EXPECTED_CLICKHOUSE_VERSIONS", "")
    for pair in filter(None, expected.split(",")):
        node, prefix = pair.split("=")
        assert versions.get(node, "").startswith(prefix), (
            f"{node} runs {versions.get(node)!r}, expected {prefix}*"
        )


@pytest.fixture(scope="module")
def errors_rows() -> None:
    conn = _connection()
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)

    rows = []
    for i, event_id in enumerate(EVENT_IDS):
        group_id = UNHANDLED_GROUP if i < EVENTS_PER_GROUP else HANDLED_GROUP
        position = i % EVENTS_PER_GROUP  # 0 is the newest event in its group
        handled = 0 if (group_id == UNHANDLED_GROUP and position == 0) else 1
        environment = "production" if group_id == UNHANDLED_GROUP else "staging"
        exc_type = "ValueError" if group_id == UNHANDLED_GROUP else "KeyError"
        ts = (base - timedelta(minutes=position)).strftime("%Y-%m-%d %H:%M:%S")
        rows.append(
            f"({PROJECT_ID}, '{ts}', '{event_id}', {group_id}, '{event_id}', 90, 0, "
            f"'{ts}', '{ts}', 0, {i}, "
            f"['environment', 'level'], ['{environment}', 'error'], "
            f"['{exc_type}'], ['boom'], ['generic'], [{handled}])"
        )

    conn.execute(
        "INSERT INTO errors_dist (project_id, timestamp, event_id, group_id, primary_hash, "
        "retention_days, deleted, received, message_timestamp, partition, offset, "
        "`tags.key`, `tags.value`, `exception_stacks.type`, `exception_stacks.value`, "
        "`exception_stacks.mechanism_type`, `exception_stacks.mechanism_handled`) VALUES "
        + ", ".join(rows)
    )
    # Distributed inserts are asynchronous by default; push them to the shards.
    conn.execute("SYSTEM FLUSH DISTRIBUTED errors_dist")

    # The default layout has two storage shards; the single-shard CI cells use
    # remote_servers_single_shard.xml. Either way every shard should get rows.
    shard_count = int(
        conn.execute(
            "SELECT count(DISTINCT shard_num) FROM system.clusters "
            "WHERE cluster = 'storage_cluster'"
        ).results[0][0]
    )
    shards = conn.execute(
        f"SELECT _shard_num, count() FROM errors_dist WHERE project_id = {PROJECT_ID} "
        "GROUP BY _shard_num ORDER BY _shard_num"
    ).results
    assert len(shards) == shard_count, (
        f"expected rows on all {shard_count} storage shards, got {shards}"
    )


MIXED_VERSION_QUERIES = [
    pytest.param(
        # Plain columns only: expected to keep working across versions.
        "SELECT (group_id AS _snuba_group_id), (count() AS _snuba_count) "
        f"FROM errors_dist WHERE {CONDITIONS} "
        "GROUP BY _snuba_group_id ORDER BY _snuba_group_id ASC",
        [(UNHANDLED_GROUP, EVENTS_PER_GROUP), (HANDLED_GROUP, EVENTS_PER_GROUP)],
        id="plain-columns",
    ),
    pytest.param(
        # The query from INC-2695 (referrer group.unhandled-flag, SENTRY-60ED).
        "SELECT (group_id AS _snuba_group_id), "
        "(argMax(has(exception_stacks.mechanism_handled, 0), timestamp) AS _snuba_unhandled) "
        f"FROM errors_dist WHERE {CONDITIONS} "
        "GROUP BY _snuba_group_id ORDER BY _snuba_group_id ASC",
        [(UNHANDLED_GROUP, 1), (HANDLED_GROUP, 0)],
        id="nested-unhandled-flag",
    ),
    pytest.param(
        # Tag keys, as used to list the tags for an issue.
        "SELECT (arrayJoin(tags.key) AS _snuba_tags_key), (count() AS _snuba_count) "
        f"FROM errors_dist WHERE {CONDITIONS} "
        "GROUP BY _snuba_tags_key ORDER BY _snuba_tags_key ASC",
        [("environment", 2 * EVENTS_PER_GROUP), ("level", 2 * EVENTS_PER_GROUP)],
        id="nested-tag-keys",
    ),
    pytest.param(
        # Tag values, through Snuba's tags[...] lookup.
        "SELECT (arrayElement(tags.value, indexOf(tags.key, 'environment')) AS _snuba_tags_environment), "
        "(count() AS _snuba_count) "
        f"FROM errors_dist WHERE {CONDITIONS} AND has(tags.key, 'environment') "
        "GROUP BY _snuba_tags_environment ORDER BY _snuba_tags_environment ASC",
        [("production", EVENTS_PER_GROUP), ("staging", EVENTS_PER_GROUP)],
        id="nested-tag-values",
    ),
    pytest.param(
        "SELECT (arrayJoin(exception_stacks.type) AS _snuba_exception_type), "
        "(uniq(group_id) AS _snuba_groups) "
        f"FROM errors_dist WHERE {CONDITIONS} "
        "GROUP BY _snuba_exception_type ORDER BY _snuba_exception_type ASC",
        [("KeyError", 1), ("ValueError", 1)],
        id="nested-exception-type",
    ),
]


@pytest.mark.parametrize("sql, expected", MIXED_VERSION_QUERIES)
def test_query_through_distributed_tables(
    request: pytest.FixtureRequest,
    errors_rows: None,
    sql: str,
    expected: list[tuple[Any, ...]],
) -> None:
    """
    Raw SQL, bypassing Snuba's remote-header fallback, so in a mixed cluster
    the nested-* queries still hit ClickHouse#113250. There they are strict
    expected failures: only NOT_FOUND_COLUMN_IN_BLOCK counts, and a pass fails
    the test, which means ClickHouse fixed the mismatch and the fallback in
    snuba/clickhouse/remote_header_fallback.py can be removed.
    """
    if not (_is_mixed_cluster() and request.node.callspec.id.startswith("nested-")):
        assert _normalize(_connection().execute(sql).results) == expected
        return

    try:
        _connection().execute(sql)
    except ClickhouseError as e:
        if e.code != ErrorCodes.NOT_FOUND_COLUMN_IN_BLOCK:
            raise
        pytest.xfail(f"ClickHouse#113250 still present: {e.message[:300]}")
    pytest.fail(
        "Query succeeded on a mixed-version cluster: ClickHouse#113250 looks fixed for "
        "this version pair. Remove this expected failure, and check whether the "
        "remote-header fallback is still needed."
    )


# The same queries as Snuba ASTs, run through execute_query with the errors
# storage's reader. In a mixed cluster the first attempt fails with
# NOT_FOUND_COLUMN_IN_BLOCK and the remote-header fallback retries it as a
# projection subquery; on a single-version cluster no retry should happen.
def _col(name: str, alias: str | None = None) -> Column:
    return Column(alias, None, name)


def _f(name: str, *args: Expression, alias: str | None = None) -> FunctionCall:
    return FunctionCall(alias, name, args)


def _snuba_query(
    selected: list[tuple[str, Expression]],
    groupby: Expression,
    extra_condition: Expression | None = None,
) -> ClickhouseQuery:
    now = datetime.now(UTC).replace(tzinfo=None)
    conditions: list[Expression] = [
        _f("in", _col("project_id"), _f("tuple", Literal(None, PROJECT_ID))),
        _f("equals", _col("deleted"), Literal(None, 0)),
        _f("greaterOrEquals", _col("timestamp"), Literal(None, now - timedelta(days=1))),
        _f("less", _col("timestamp"), Literal(None, now + timedelta(hours=1))),
    ]
    if extra_condition is not None:
        conditions.append(extra_condition)
    condition = conditions[0]
    for c in conditions[1:]:
        condition = _f("and", condition, c)
    return ClickhouseQuery(
        Table("errors_dist", ColumnSet([]), storage_key=StorageKey("errors")),
        selected_columns=[SelectedExpression(name, exp) for name, exp in selected],
        condition=condition,
        groupby=[groupby],
        order_by=[OrderBy(OrderByDirection.ASC, groupby)],
    )


_TAG_KEY = _f("arrayJoin", _col("tags.key"), alias="_snuba_tags_key")
_TAG_ENV = _f(
    "arrayElement",
    _col("tags.value"),
    _f("indexOf", _col("tags.key"), Literal(None, "environment")),
    alias="_snuba_tags_environment",
)
_EXC_TYPE = _f("arrayJoin", _col("exception_stacks.type"), alias="_snuba_exception_type")

SNUBA_PATH_QUERIES = [
    pytest.param(
        lambda: _snuba_query(
            [
                ("_snuba_group_id", _col("group_id", "_snuba_group_id")),
                (
                    "_snuba_unhandled",
                    _f(
                        "argMax",
                        _f("has", _col("exception_stacks.mechanism_handled"), Literal(None, 0)),
                        _col("timestamp"),
                        alias="_snuba_unhandled",
                    ),
                ),
            ],
            _col("group_id", "_snuba_group_id"),
        ),
        [(UNHANDLED_GROUP, 1), (HANDLED_GROUP, 0)],
        id="nested-unhandled-flag",
    ),
    pytest.param(
        lambda: _snuba_query(
            [("_snuba_tags_key", _TAG_KEY), ("_snuba_count", _f("count", alias="_snuba_count"))],
            _TAG_KEY,
        ),
        [("environment", 2 * EVENTS_PER_GROUP), ("level", 2 * EVENTS_PER_GROUP)],
        id="nested-tag-keys",
    ),
    pytest.param(
        lambda: _snuba_query(
            [
                ("_snuba_tags_environment", _TAG_ENV),
                ("_snuba_count", _f("count", alias="_snuba_count")),
            ],
            _TAG_ENV,
            _f("has", _col("tags.key"), Literal(None, "environment")),
        ),
        [("production", EVENTS_PER_GROUP), ("staging", EVENTS_PER_GROUP)],
        id="nested-tag-values",
    ),
    pytest.param(
        lambda: _snuba_query(
            [
                ("_snuba_exception_type", _EXC_TYPE),
                ("_snuba_groups", _f("uniq", _col("group_id"), alias="_snuba_groups")),
            ],
            _EXC_TYPE,
        ),
        [("KeyError", 1), ("ValueError", 1)],
        id="nested-exception-type",
    ),
]


@pytest.mark.parametrize("build_query, expected", SNUBA_PATH_QUERIES)
def test_query_through_snuba_with_fallback(
    errors_rows: None,
    build_query: Callable[[], ClickhouseQuery],
    expected: list[tuple[Any, ...]],
) -> None:
    query = build_query()
    stats: dict[str, Any] = {}
    result = execute_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        formatted_query=format_query(query),
        reader=get_storage(StorageKey.ERRORS).get_cluster().get_reader(),
        timer=Timer("test"),
        stats=stats,
        clickhouse_query_settings={},
        robust=False,
    )
    rows = _normalize([tuple(row.values()) for row in result["data"]])
    print(f"remote_header_fallback={stats.get('remote_header_fallback', False)}")
    assert rows == expected
    if os.environ.get("EXPECTED_CLICKHOUSE_VERSIONS"):
        assert stats.get("remote_header_fallback", False) is _is_mixed_cluster()
