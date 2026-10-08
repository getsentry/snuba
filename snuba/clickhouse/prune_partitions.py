"""
Forget ``block_numbers`` znodes of dead partitions on ReplicatedMergeTree tables.

When both ``enable_block_number_column`` and ``enable_block_offset_column`` are
set, ClickHouse 25.8 lists every child of ``<zookeeper_path>/block_numbers`` on
each replication queue scheduling pass. Those znodes are never removed, even
after a partition is dropped or expires, so the scan grows with every partition
a table has ever had. ``ALTER TABLE ... FORGET PARTITION`` removes them.

A partition is only forgotten when it is past retention (with a margin) and
nothing on any replica of the shard still references it. Every check fails
closed: an error while checking skips the table.
"""

import logging
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import Enum

from snuba.clickhouse.escaping import escape_identifier
from snuba.clickhouse.pool import ClickhousePool
from snuba.utils.metrics.backends.abstract import MetricsBackend

logger = logging.getLogger("snuba.clickhouse.prune_partitions")

DEFAULT_BATCH_SIZE = 20
DEFAULT_RETENTION_MARGIN = timedelta(days=14)

# Partition IDs of a (retention_days, toMonday(timestamp)) key, e.g. 90-20260601.
_PARTITION_ID = re.compile(r"^(\d+)-(\d{8})$")

# partition_key as rendered by system.tables, e.g. (retention_days, toMonday(timestamp)).
_PARTITION_KEY = re.compile(r"^\(?\s*\w+\s*,\s*toMonday\(\s*\w+\s*\)\s*\)?$")

_BLOCK_FLAGS = ("enable_block_number_column = 1", "enable_block_offset_column = 1")

PartitionCallback = Callable[[str], None]


class Category(Enum):
    PATCH = "patch"
    UNPARSEABLE = "unparseable"
    FUTURE_GARBAGE = "future_garbage"
    LIVE = "live"
    DEAD_PAST = "dead_past"


class SkipTable(Exception):
    """Raised when a table must not be pruned."""


class ForgetError(Exception):
    """Raised when FORGET PARTITION fails, which means an eligibility check was wrong."""

    def __init__(self, result: "TablePruneResult", partition_id: str) -> None:
        super().__init__(f"FORGET PARTITION {partition_id} failed on {result.table}")
        self.result = result
        self.partition_id = partition_id


@dataclass
class PartitionReport:
    partition_id: str
    category: Category
    blockers: Sequence[str] = ()
    forgotten: bool = False

    @property
    def eligible(self) -> bool:
        return self.category == Category.DEAD_PAST and not self.blockers


@dataclass
class TablePruneResult:
    table: str
    shard: int | None = None
    skipped_reason: str | None = None
    partitions: list[PartitionReport] = field(default_factory=list)

    @property
    def eligible_count(self) -> int:
        return sum(1 for p in self.partitions if p.eligible)

    @property
    def forgotten_count(self) -> int:
        return sum(1 for p in self.partitions if p.forgotten)

    @property
    def expected_scan_size(self) -> int:
        """getChildren calls per scheduling pass once forgotten znodes are gone."""
        return 1 + len(self.partitions) - self.forgotten_count


def today() -> date:
    return datetime.now(UTC).date()


def parse_partition_id(partition_id: str) -> tuple[int, date] | None:
    """Read ``(retention_days, week_start)`` from an ID such as ``90-20260601``."""
    match = _PARTITION_ID.match(partition_id)
    if match is None:
        return None
    try:
        week_start = datetime.strptime(match.group(2), "%Y%m%d").date()
    except ValueError:
        return None
    return int(match.group(1)), week_start


def classify_partition(
    partition_id: str,
    today: date,
    retention_margin: timedelta = DEFAULT_RETENTION_MARGIN,
) -> Category:
    if partition_id.startswith("patch-"):
        return Category.PATCH
    parsed = parse_partition_id(partition_id)
    if parsed is None:
        return Category.UNPARSEABLE
    retention_days, week_start = parsed
    if week_start > today:
        return Category.FUTURE_GARBAGE
    expires = week_start + timedelta(days=7 + retention_days) + retention_margin
    if expires < today:
        return Category.DEAD_PAST
    return Category.LIVE


def _system_table(cluster_name: str | None, table: str) -> str:
    if cluster_name is None:
        return f"system.{table}"
    return f"clusterAllReplicas(%(cluster)s, system.{table})"


def _shard_filter(cluster_name: str | None) -> str:
    return "" if cluster_name is None else "AND shardNum() = %(shard)s"


def get_zookeeper_path(clickhouse: ClickhousePool, database: str, table: str) -> str:
    result = clickhouse.execute(
        "SELECT zookeeper_path FROM system.replicas "
        "WHERE database = %(database)s AND table = %(table)s",
        {"database": database, "table": table},
    )
    if not result.results:
        raise SkipTable("not a replicated table")
    return str(result.results[0][0])


def check_table_settings(clickhouse: ClickhousePool, database: str, table: str) -> None:
    result = clickhouse.execute(
        "SELECT engine_full, partition_key FROM system.tables "
        "WHERE database = %(database)s AND name = %(table)s",
        {"database": database, "table": table},
    )
    if not result.results:
        raise SkipTable("table does not exist")
    engine_full, partition_key = result.results[0]
    if not all(flag in engine_full for flag in _BLOCK_FLAGS):
        raise SkipTable("block number and offset columns are not both enabled")
    if not _PARTITION_KEY.match(partition_key):
        raise SkipTable(f"unsupported partition key {partition_key!r}")


def get_local_shard(clickhouse: ClickhousePool, cluster_name: str) -> int:
    result = clickhouse.execute(
        "SELECT DISTINCT shard_num FROM system.clusters WHERE cluster = %(cluster)s AND is_local",
        {"cluster": cluster_name},
    )
    shards = [shard for (shard,) in result.results]
    if len(shards) != 1:
        raise SkipTable(f"expected one local shard in {cluster_name}, found {shards}")
    return int(shards[0])


def list_block_number_partitions(clickhouse: ClickhousePool, zookeeper_path: str) -> list[str]:
    result = clickhouse.execute(
        "SELECT name FROM system.zookeeper WHERE path = %(path)s ORDER BY name",
        {"path": f"{zookeeper_path}/block_numbers"},
    )
    return [name for (name,) in result.results]


def find_blockers(
    clickhouse: ClickhousePool,
    *,
    database: str,
    table: str,
    zookeeper_path: str,
    cluster_name: str | None,
    shard: int | None,
    partition_ids: Sequence[str],
) -> dict[str, list[str]]:
    """
    Return the checks each partition fails. Partitions passing every check are
    absent. Replica state is read from every replica of the shard, and an
    unreachable replica raises rather than being skipped.
    """
    blockers: dict[str, list[str]] = {}
    if not partition_ids:
        return blockers

    params = {
        "database": database,
        "table": table,
        "cluster": cluster_name,
        "shard": shard,
        "partition_ids": list(partition_ids),
    }
    shard_filter = _shard_filter(cluster_name)
    scope = f"database = %(database)s AND table = %(table)s {shard_filter}"

    checks = {
        # Parts in any state, not only active ones.
        "parts": (
            f"SELECT DISTINCT partition_id FROM {_system_table(cluster_name, 'parts')} "
            f"WHERE {scope} AND has(%(partition_ids)s, partition_id)"
        ),
        "detached_parts": (
            f"SELECT DISTINCT partition_id FROM {_system_table(cluster_name, 'detached_parts')} "
            f"WHERE {scope} AND has(%(partition_ids)s, partition_id)"
        ),
        "replication_queue": (
            f"SELECT DISTINCT pid FROM {_system_table(cluster_name, 'replication_queue')} "
            "ARRAY JOIN %(partition_ids)s AS pid "
            f"WHERE {scope} AND ("
            "startsWith(new_part_name, concat(pid, '_')) "
            "OR arrayExists(x -> startsWith(x, concat(pid, '_')), parts_to_merge))"
        ),
        "mutations": (
            f"SELECT DISTINCT pid FROM {_system_table(cluster_name, 'mutations')} "
            "ARRAY JOIN block_numbers.partition_id AS pid "
            f"WHERE {scope} AND NOT is_done AND has(%(partition_ids)s, pid)"
        ),
    }
    for check, query in checks.items():
        for (partition_id,) in clickhouse.execute(query, params).results:
            blockers.setdefault(partition_id, []).append(check)

    # system.zookeeper needs an exact path, so block locks are read one by one.
    for partition_id in partition_ids:
        result = clickhouse.execute(
            "SELECT count() FROM system.zookeeper WHERE path = %(path)s",
            {"path": f"{zookeeper_path}/block_numbers/{partition_id}"},
        )
        if result.results and result.results[0][0]:
            blockers.setdefault(partition_id, []).append("block_locks")

    return blockers


def forget_partition(
    clickhouse: ClickhousePool, database: str, table: str, partition_id: str
) -> None:
    escaped_database = escape_identifier(database)
    escaped_table = escape_identifier(table)
    assert escaped_database is not None
    assert escaped_table is not None
    clickhouse.execute(
        f"ALTER TABLE {escaped_database}.{escaped_table} FORGET PARTITION ID %(partition_id)s",
        {"partition_id": partition_id},
    )


def prune_table(
    clickhouse: ClickhousePool,
    *,
    database: str,
    table: str,
    cluster_name: str | None,
    metrics: MetricsBackend,
    today: date,
    retention_margin: timedelta = DEFAULT_RETENTION_MARGIN,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_forgets: int,
    dry_run: bool = True,
    on_partition_forgotten: PartitionCallback | None = None,
) -> TablePruneResult:
    """
    Forget up to ``max_forgets`` eligible partitions of ``table``. Eligibility
    is re-checked for each batch right before it is forgotten. In dry run the
    result reports eligibility without forgetting anything.
    """
    result = TablePruneResult(table=table)
    tags = {"table": table}

    try:
        zookeeper_path = get_zookeeper_path(clickhouse, database, table)
        check_table_settings(clickhouse, database, table)
        if cluster_name is not None:
            result.shard = get_local_shard(clickhouse, cluster_name)

        def blockers_for(partition_ids: Sequence[str]) -> dict[str, list[str]]:
            return find_blockers(
                clickhouse,
                database=database,
                table=table,
                zookeeper_path=zookeeper_path,
                cluster_name=cluster_name,
                shard=result.shard,
                partition_ids=partition_ids,
            )

        result.partitions = [
            PartitionReport(partition_id, classify_partition(partition_id, today, retention_margin))
            for partition_id in list_block_number_partitions(clickhouse, zookeeper_path)
        ]
        metrics.gauge("block_number_znodes", len(result.partitions), tags=tags)

        candidates = [p for p in result.partitions if p.category == Category.DEAD_PAST]
        blockers = blockers_for([p.partition_id for p in candidates])
        for report in candidates:
            report.blockers = blockers.get(report.partition_id, [])
    except SkipTable as error:
        result.skipped_reason = str(error)
        metrics.increment("skipped", tags={**tags, "reason": "precondition"})
        return result
    except Exception as error:
        logger.exception("Eligibility checks failed for %s.%s", database, table)
        result.skipped_reason = f"eligibility check failed: {error}"
        metrics.increment("skipped", tags={**tags, "reason": "check_failed"})
        return result

    if dry_run:
        return result

    eligible = [p for p in result.partitions if p.eligible][:max_forgets]
    for start in range(0, len(eligible), batch_size):
        batch = eligible[start : start + batch_size]
        try:
            recheck = blockers_for([p.partition_id for p in batch])
        except Exception as error:
            logger.exception("Re-check failed for %s.%s", database, table)
            result.skipped_reason = f"re-check failed: {error}"
            metrics.increment("skipped", tags={**tags, "reason": "check_failed"})
            return result

        for report in batch:
            if report.partition_id in recheck:
                report.blockers = recheck[report.partition_id]
                metrics.increment("skipped", tags={**tags, "reason": "recheck_blocked"})
                continue
            try:
                forget_partition(clickhouse, database, table, report.partition_id)
            except Exception as error:
                metrics.increment("forget_failed", tags=tags)
                raise ForgetError(result, report.partition_id) from error
            report.forgotten = True
            metrics.increment("forgotten", tags=tags)
            if on_partition_forgotten is not None:
                on_partition_forgotten(report.partition_id)

    return result
