from copy import deepcopy
from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    Column,
    DateTime,
    LowCardinality,
    Materialized,
    Nested,
    String,
    UInt,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines

UNKNOWN_SPAN_STATUS = 2

tags_col = Column("tags", Nested([("key", String()), ("value", String())]))

columns = [
    Column("project_id", UInt(64)),
    Column("trace_id", UUID()),
    Column("transaction_span_id", UInt(64)),
    Column("span_id", UInt(64)),
    Column("parent_span_id", UInt(64)),
    Column("transaction_name", LowCardinality(String())),
    Column("description", LowCardinality(String())),  # description in span
    Column("op", LowCardinality(String())),
    Column("status", WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS)),),
    Column("start_ts", DateTime()),
    Column("start_ns", UInt(32)),
    Column("finish_ts", DateTime()),
    Column("finish_ns", UInt(32)),
    Column("duration", UInt(32)),
    tags_col,
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_experimental_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.TRANSACTIONS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(finish_ts), cityHash64(transaction_span_id), op, cityHash64(toString(trace_id)), cityHash64(span_id))",
                    partition_by="(toMonday(finish_ts))",
                    sample_by="cityHash64(span_id)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_experimental_local",
                column=Column(
                    "_tags_hash_map",
                    Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN),
                ),
                after="tags.value",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_experimental_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        dest_index = columns.index(tags_col)
        dist_cols = deepcopy(columns)
        dist_cols.insert(dest_index, Column("_tags_hash_map", Array(UInt(64))))

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_experimental_dist",
                columns=dist_cols,
                engine=table_engines.Distributed(
                    local_table_name="spans_experimental_local",
                    sharding_key="cityHash64(transaction_span_id)",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_experimental_dist",
            )
        ]
