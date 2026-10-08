from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from typing import Any
from unittest.mock import Mock, call

import pytest

from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult
from snuba.clickhouse.prune_partitions import (
    Category,
    ForgetError,
    classify_partition,
    parse_partition_id,
    prune_table,
)
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

TODAY = date(2026, 10, 8)
ZK_PATH = "/clickhouse/tables/eap/2/default/eap_items_2_local"
ENGINE_FULL = (
    "ReplicatedReplacingMergeTree(...) SETTINGS index_granularity = 8192, "
    "enable_block_number_column = 1, enable_block_offset_column = 1"
)
PARTITION_KEY = "(retention_days, toMonday(timestamp))"

DEAD = "30-20260105"
DEAD_2 = "90-19700101"
LIVE = "90-20260803"
FUTURE = "30-20991228"
PATCH = "patch-abc123-30-20260105"


@pytest.mark.parametrize(
    "partition_id, expected",
    [
        ("90-20260601", (90, date(2026, 6, 1))),
        ("90-19700101", (90, date(1970, 1, 1))),
        ("90-20261301", None),
        ("a3f1c2d4e5b6", None),
        ("20260601", None),
    ],
)
def test_parse_partition_id(partition_id: str, expected: tuple[int, date] | None) -> None:
    assert parse_partition_id(partition_id) == expected


@pytest.mark.parametrize(
    "partition_id, expected",
    [
        (PATCH, Category.PATCH),
        ("a3f1c2d4e5b6", Category.UNPARSEABLE),
        (FUTURE, Category.FUTURE_GARBAGE),
        (LIVE, Category.LIVE),
        (DEAD, Category.DEAD_PAST),
        (DEAD_2, Category.DEAD_PAST),
    ],
)
def test_classify_partition(partition_id: str, expected: Category) -> None:
    assert classify_partition(partition_id, TODAY) == expected


def test_classify_partition_margin_boundary() -> None:
    # 2026-08-31 + 7 days + 30 days retention + 14 days margin = 2026-10-21.
    assert classify_partition("30-20260831", date(2026, 10, 21)) == Category.LIVE
    assert classify_partition("30-20260831", date(2026, 10, 22)) == Category.DEAD_PAST
    assert classify_partition("30-20260831", date(2026, 10, 8), timedelta(0)) == Category.DEAD_PAST


Responder = Callable[[str, Mapping[str, Any]], Sequence[tuple[Any, ...]]]


class FakeClickhouse:
    """Answers queries by matching on their text so tests don't depend on order."""

    def __init__(
        self,
        znodes: Sequence[str],
        *,
        replicated: bool = True,
        engine_full: str = ENGINE_FULL,
        partition_key: str = PARTITION_KEY,
        blockers: Mapping[str, Sequence[str]] | None = None,
        block_locks: Sequence[str] = (),
        forget_fails: Sequence[str] = (),
        parts_by_replica: Mapping[tuple[int, int], Sequence[str]] | None = None,
    ) -> None:
        self.znodes = znodes
        self.replicated = replicated
        self.engine_full = engine_full
        self.partition_key = partition_key
        # check name -> partition IDs that fail it
        self.blockers = dict(blockers or {})
        self.block_locks = set(block_locks)
        self.forget_fails = set(forget_fails)
        # (shard_num, replica_num) -> partition IDs with parts on that replica
        self.parts_by_replica = dict(parts_by_replica or {})
        self.mock = Mock(spec=ClickhousePool)
        self.mock.execute.side_effect = self.execute

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> ClickhouseResult:
        params = params or {}
        if "FORGET PARTITION" in query:
            if params["partition_id"] in self.forget_fails:
                raise RuntimeError("Code: 1. FORGET failed")
            return ClickhouseResult()
        if "FROM system.replicas" in query:
            return ClickhouseResult([(ZK_PATH,)] if self.replicated else [])
        if "FROM system.tables" in query:
            return ClickhouseResult([(self.engine_full, self.partition_key)])
        if "FROM system.clusters" in query:
            return ClickhouseResult([(2,)])
        if "FROM system.zookeeper" in query and "count()" in query:
            partition_id = params["path"].rsplit("/", 1)[1]
            return ClickhouseResult([(1 if partition_id in self.block_locks else 0,)])
        if "FROM system.zookeeper" in query:
            assert params["path"] == f"{ZK_PATH}/block_numbers"
            return ClickhouseResult([(z,) for z in self.znodes])
        if "system.parts)" in query and self.parts_by_replica:
            return ClickhouseResult(
                [(p,) for p in params["partition_ids"] if p in self._replica_parts(query, params)]
            )
        for check in ("detached_parts", "parts", "replication_queue", "mutations"):
            if f"system.{check})" in query or f"system.{check} " in query:
                failing = self.blockers.get(check, ())
                return ClickhouseResult([(p,) for p in params["partition_ids"] if p in failing])
        raise AssertionError(f"unexpected query: {query}")

    def _replica_parts(self, query: str, params: Mapping[str, Any]) -> set[str]:
        # clusterAllReplicas renumbers every replica as its own shard, so a
        # shardNum() filter selects one replica by its position, not a shard.
        replicas = sorted(self.parts_by_replica)
        if "shardNum()" in query:
            replicas = replicas[params["shard"] - 1 : params["shard"]]
        return {p for r in replicas for p in self.parts_by_replica[r]}

    def forgets(self) -> list[str]:
        return [
            c.args[1]["partition_id"]
            for c in self.mock.execute.call_args_list
            if "FORGET PARTITION" in c.args[0]
        ]


def run(clickhouse: FakeClickhouse, **kwargs: Any) -> Any:
    options: dict[str, Any] = {
        "database": "default",
        "table": "eap_items_2_local",
        "cluster_name": "eap",
        "metrics": DummyMetricsBackend(),
        "today": TODAY,
        "max_forgets": 100,
        "dry_run": False,
    }
    options.update(kwargs)
    return prune_table(clickhouse.mock, **options)


ALL = [DEAD, DEAD_2, LIVE, FUTURE, PATCH, "a3f1c2d4e5b6"]


def test_dry_run_reports_without_forgetting() -> None:
    clickhouse = FakeClickhouse(ALL)

    result = run(clickhouse, dry_run=True)

    assert clickhouse.forgets() == []
    assert result.shard == 2
    assert result.skipped_reason is None
    assert result.eligible_count == 2
    assert result.expected_scan_size == 1 + len(ALL)
    assert {p.partition_id: p.category for p in result.partitions}[PATCH] == Category.PATCH


def test_forgets_only_eligible_partitions() -> None:
    clickhouse = FakeClickhouse(ALL)

    result = run(clickhouse)

    assert sorted(clickhouse.forgets()) == sorted([DEAD, DEAD_2])
    assert result.forgotten_count == 2
    assert result.expected_scan_size == 1 + len(ALL) - 2
    assert (
        call(
            "ALTER TABLE default.eap_items_2_local FORGET PARTITION ID %(partition_id)s",
            {"partition_id": DEAD},
        )
        in clickhouse.mock.execute.call_args_list
    )


def test_escapes_identifiers() -> None:
    clickhouse = FakeClickhouse([DEAD])

    run(clickhouse, database="my-db")

    assert any(
        c.args[0].startswith("ALTER TABLE `my-db`.eap_items_2_local FORGET")
        for c in clickhouse.mock.execute.call_args_list
    )


@pytest.mark.parametrize("check", ["parts", "detached_parts", "replication_queue", "mutations"])
def test_replica_state_blocks_forget(check: str) -> None:
    clickhouse = FakeClickhouse([DEAD, DEAD_2], blockers={check: [DEAD]})

    result = run(clickhouse)

    assert clickhouse.forgets() == [DEAD_2]
    blocked = next(p for p in result.partitions if p.partition_id == DEAD)
    assert blocked.blockers == [check]
    assert not blocked.eligible


def test_block_locks_block_forget() -> None:
    clickhouse = FakeClickhouse([DEAD, DEAD_2], block_locks=[DEAD])

    run(clickhouse)

    assert clickhouse.forgets() == [DEAD_2]


def test_checks_read_every_replica_of_the_cluster() -> None:
    clickhouse = FakeClickhouse([DEAD])

    run(clickhouse)

    parts_query = next(
        c for c in clickhouse.mock.execute.call_args_list if "system.parts)" in c.args[0]
    )
    assert "clusterAllReplicas(%(cluster)s, system.parts)" in parts_query.args[0]
    assert "shardNum()" not in parts_query.args[0]
    assert "active" not in parts_query.args[0]
    assert parts_query.args[1]["cluster"] == "eap"


@pytest.mark.parametrize(
    "replica",
    [
        pytest.param((2, 2), id="sibling replica"),
        pytest.param((1, 1), id="other shard"),
        pytest.param((3, 2), id="last replica"),
    ],
)
def test_parts_on_any_replica_block_forget(replica: tuple[int, int]) -> None:
    # Local node is on shard 2 of a 3 shard x 2 replica cluster.
    parts_by_replica: dict[tuple[int, int], Sequence[str]] = {
        (s, r): [] for s in (1, 2, 3) for r in (1, 2)
    }
    parts_by_replica[replica] = [DEAD]
    clickhouse = FakeClickhouse([DEAD, DEAD_2], parts_by_replica=parts_by_replica)

    result = run(clickhouse)

    assert clickhouse.forgets() == [DEAD_2]
    assert next(p for p in result.partitions if p.partition_id == DEAD).blockers == ["parts"]


def test_single_node_reads_local_system_tables() -> None:
    clickhouse = FakeClickhouse([DEAD])

    result = run(clickhouse, cluster_name=None)

    assert result.shard is None
    assert clickhouse.forgets() == [DEAD]
    assert not any(
        "clusterAllReplicas" in c.args[0] for c in clickhouse.mock.execute.call_args_list
    )


def test_recheck_blocks_partition_that_gained_parts() -> None:
    clickhouse = FakeClickhouse([DEAD, DEAD_2])
    first_check = True

    original = clickhouse.execute

    def execute(query: str, params: Mapping[str, Any] | None = None) -> ClickhouseResult:
        nonlocal first_check
        if "system.parts)" in query:
            if first_check:
                first_check = False
            else:
                clickhouse.blockers["parts"] = [DEAD]
        return original(query, params)

    clickhouse.mock.execute.side_effect = execute

    result = run(clickhouse)

    assert clickhouse.forgets() == [DEAD_2]
    assert next(p for p in result.partitions if p.partition_id == DEAD).blockers == ["parts"]


def test_rechecks_each_batch() -> None:
    clickhouse = FakeClickhouse([DEAD, DEAD_2, "30-20260112"])

    run(clickhouse, batch_size=2)

    parts_checks = [
        c.args[1]["partition_ids"]
        for c in clickhouse.mock.execute.call_args_list
        if "system.parts)" in c.args[0]
    ]
    # One initial check of all candidates, then one per batch.
    assert len(parts_checks) == 3
    assert [len(ids) for ids in parts_checks[1:]] == [2, 1]


def test_max_forgets_caps_the_run() -> None:
    clickhouse = FakeClickhouse([DEAD, DEAD_2, "30-20260112"])

    result = run(clickhouse, max_forgets=2)

    assert len(clickhouse.forgets()) == 2
    assert result.forgotten_count == 2


def test_forget_error_stops_the_run() -> None:
    clickhouse = FakeClickhouse([DEAD_2, DEAD, "30-20260112"], forget_fails=[DEAD])

    with pytest.raises(ForgetError) as error:
        run(clickhouse)

    assert error.value.partition_id == DEAD
    # DEAD_2 was forgotten, DEAD was attempted and failed, the rest never ran.
    assert clickhouse.forgets() == [DEAD_2, DEAD]
    assert error.value.result.forgotten_count == 1


@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"replicated": False}, "not a replicated table"),
        (
            {"engine_full": "ReplicatedMergeTree SETTINGS enable_block_number_column = 1"},
            "block number and offset columns are not both enabled",
        ),
        ({"partition_key": "toStartOfDay(timestamp)"}, "unsupported partition key"),
    ],
)
def test_skips_tables_failing_preconditions(kwargs: dict[str, Any], reason: str) -> None:
    clickhouse = FakeClickhouse([DEAD], **kwargs)

    result = run(clickhouse)

    assert result.skipped_reason is not None
    assert result.skipped_reason.startswith(reason)
    assert clickhouse.forgets() == []


def test_check_error_skips_table() -> None:
    clickhouse = FakeClickhouse([DEAD])
    original = clickhouse.execute

    def execute(query: str, params: Mapping[str, Any] | None = None) -> ClickhouseResult:
        if "system.mutations)" in query:
            raise RuntimeError("Code: 279. ALL_CONNECTION_TRIES_FAILED")
        return original(query, params)

    clickhouse.mock.execute.side_effect = execute

    result = run(clickhouse)

    assert result.skipped_reason is not None
    assert "eligibility check failed" in result.skipped_reason
    assert clickhouse.forgets() == []
