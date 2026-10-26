"""Create the eap_items_2 tables.

Step 1 of moving eap_items onto a ReplacingMergeTree keyed on `version`.
This migration only creates the new, empty tables; nothing reads or writes
them yet. Populating them is a separate, manual step: existing parts are
attached from the eap_items_1 tables with the `snuba attach-partitions`
tooling, and reads are cut over later behind a sentry-option gate.

The new tables are created with ``CREATE TABLE ... AS <v1 table>`` rather
than an explicit column list. That is deliberate and load-bearing:

* ``ATTACH PARTITION`` requires the source and destination to have the same
  physical columns and types. eap_items_1_local currently has 184 columns
  accumulated over ~40 migrations, so restating them here would be a large
  hand-transcription with a silent, painful failure mode. Deriving the
  structure from the live v1 table makes the parity structural rather than
  something a reviewer has to verify column by column.
* ``AS`` copies the data skipping indexes too, so the bloom filters on
  trace_id / indexed_name / conversation_id / session_id / ai_conversation_id
  come along without being restated.

The engine is the one thing that intentionally differs: v1 is a plain
``ReplacingMergeTree`` while v2 is ``ReplacingMergeTree(version)``. The sort
key, primary key, partition key, TTL and index granularity are all specified
to match v1 exactly, since those *are* checked when attaching parts. A
differing engine is not: attaching from ``ReplacingMergeTree`` into
``ReplacingMergeTree(version)`` is permitted and preserves the column.

This follows the ``CREATE TABLE ... AS`` pattern already used by
0056_eap_items_dist_ro.
"""

from collections.abc import Sequence

from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

# Sharding key used by every eap_items distributed table. Must stay in step
# with 0024_items / 0056_eap_items_dist_ro, otherwise a shard would land the
# same trace in a different place than v1 did.
SHARDING_KEY = "cityHash64(reinterpretAsUInt128(trace_id))"

# Structure shared by eap_items_1_local and all three of its downsample
# tiers, confirmed against a fully migrated database. Restated here because
# `CREATE TABLE ... AS` copies columns and indexes but not the engine clause.
PRIMARY_KEY = "(organization_id, project_id, item_type, timestamp)"
ORDER_BY = "(organization_id, project_id, item_type, timestamp, trace_id, item_id)"
PARTITION_BY = "(retention_days, toMonday(timestamp))"
# The downsample tiers keep `retention_days` as the TTL column; their views
# project `downsampled_retention_days AS retention_days`, so the column name
# is the same on every tier even though the value differs.
TTL = "timestamp + toIntervalDay(retention_days)"
SETTINGS = {"index_granularity": "8192"}

VERSION_COLUMN = "version"

# (v2 prefix, v1 prefix) for the base table and each downsample tier.
TABLE_PREFIXES: list[tuple[str, str]] = [
    ("eap_items_2", "eap_items_1"),
    ("eap_items_2_downsample_8", "eap_items_1_downsample_8"),
    ("eap_items_2_downsample_64", "eap_items_1_downsample_64"),
    ("eap_items_2_downsample_512", "eap_items_1_downsample_512"),
]


def _on_cluster_clause(distributed: bool) -> str:
    cluster = get_cluster(storage_set)
    if cluster.is_single_node():
        return ""
    name = (
        cluster.get_clickhouse_distributed_cluster_name()
        if distributed
        else cluster.get_clickhouse_cluster_name()
    )
    return f" ON CLUSTER `{name}`" if name else ""


def _create_local_sql(new_table: str, source_table: str) -> str:
    engine = table_engines.ReplacingMergeTree(
        storage_set=storage_set,
        version_column=VERSION_COLUMN,
        primary_key=PRIMARY_KEY,
        order_by=ORDER_BY,
        partition_by=PARTITION_BY,
        ttl=TTL,
        settings=SETTINGS,
    ).get_sql(get_cluster(storage_set), new_table)
    return (
        f"CREATE TABLE IF NOT EXISTS {new_table}{_on_cluster_clause(distributed=False)} "
        f"AS {source_table} ENGINE {engine}"
    )


def _create_dist_sql(new_table: str, source_dist_table: str, local_table: str) -> str:
    # Templated off the *v1 distributed* table, not off the v2 local table.
    # Two reasons, both load-bearing:
    #
    # 1. Topology. Distributed tables are created ON CLUSTER over the query
    #    nodes, which do not host the local tables -- those live on the
    #    storage cluster. `AS eap_items_2_local` therefore fails on the query
    #    nodes with "Table ... does not exist", even though the local table
    #    was created successfully moments earlier on the storage nodes.
    #    0056_eap_items_dist_ro templates off the dist tables for the same
    #    reason.
    #
    # 2. Column defaults. eap_items_1_dist carries
    #    `version DEFAULT toUnixTimestamp64Milli(now64(3))` while
    #    eap_items_1_local carries `version DEFAULT 0`. A Distributed table
    #    materialises its *own* defaults on insert and ships the result to
    #    the shard, so copying the local table's `DEFAULT 0` here would make
    #    every live write land with version 0 and be unable to supersede an
    #    attached legacy row. Templating off the v1 dist table inherits the
    #    right default for free.
    #
    # Note this means the downsample tier dist tables have no `version`
    # column at all, mirroring their v1 counterparts, which only ever gained
    # the column on `*_local`. That is intended: nothing inserts through the
    # tier dist tables (the materialized views write to `*_local`) and
    # nothing queries `version`.
    #
    # The Distributed engine clause is assembled by hand rather than via
    # table_engines.Distributed, which asserts the cluster is multi-node.
    # forwards_ops() is evaluated even in single-node environments where the
    # distributed operations are then skipped, so building it eagerly through
    # that helper would raise. 0056_eap_items_dist_ro does the same.
    cluster = get_cluster(storage_set)
    cluster_name = cluster.get_clickhouse_cluster_name()
    database = cluster.get_database()
    return (
        f"CREATE TABLE IF NOT EXISTS {new_table}{_on_cluster_clause(distributed=True)} "
        f"AS {source_dist_table} "
        f"ENGINE = Distributed(`{cluster_name}`, {database}, {local_table}, {SHARDING_KEY})"
    )


def _drop_sql(table: str, distributed: bool) -> str:
    return f"DROP TABLE IF EXISTS {table}{_on_cluster_clause(distributed=distributed)}"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: list[SqlOperation] = []
        for new_prefix, source_prefix in TABLE_PREFIXES:
            ops.append(
                operations.RunSql(
                    storage_set=storage_set,
                    statement=_create_local_sql(f"{new_prefix}_local", f"{source_prefix}_local"),
                    target=OperationTarget.LOCAL,
                )
            )
            ops.append(
                operations.RunSql(
                    storage_set=storage_set,
                    statement=_create_dist_sql(
                        f"{new_prefix}_dist",
                        f"{source_prefix}_dist",
                        f"{new_prefix}_local",
                    ),
                    target=OperationTarget.DISTRIBUTED,
                )
            )
        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: list[SqlOperation] = []
        for new_prefix, _ in TABLE_PREFIXES:
            ops.append(
                operations.RunSql(
                    storage_set=storage_set,
                    statement=_drop_sql(f"{new_prefix}_dist", distributed=True),
                    target=OperationTarget.DISTRIBUTED,
                )
            )
            ops.append(
                operations.RunSql(
                    storage_set=storage_set,
                    statement=_drop_sql(f"{new_prefix}_local", distributed=False),
                    target=OperationTarget.LOCAL,
                )
            )
        return ops
