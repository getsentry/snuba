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
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

PROJECT_ID = 4500
UNHANDLED_GROUP = 101  # newest event is unhandled
HANDLED_GROUP = 102  # every event is handled
EVENTS_PER_GROUP = 6

# Fixed IDs so the cityHash64(event_id) sharding is deterministic. These put
# rows from both groups on both storage shards.
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

    shards = conn.execute(
        f"SELECT _shard_num, count() FROM errors_dist WHERE project_id = {PROJECT_ID} "
        "GROUP BY _shard_num ORDER BY _shard_num"
    ).results
    assert len(shards) == 2, f"expected rows on both storage shards, got {shards}"


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
    errors_rows: None, sql: str, expected: list[tuple[Any, ...]]
) -> None:
    assert _normalize(_connection().execute(sql).results) == expected
