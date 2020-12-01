from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.snuba_migrations.spans_experimental.columns import columns

TABLE_NAME = "spans_experimental_local"
TABLE_NAME_NEW = "spans_experimental_local_new"


class Migration(migration.MultiStepMigration):
    """
    Changes the PARTITION clause of the spans table adding retention_days.
    Unfortunately this is not an operation that can be done through an
    ALTER statement and it requires the table to be rebuilt.
    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=TABLE_NAME_NEW,
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.TRANSACTIONS,
                    version_column="deleted",
                    order_by=(
                        "(project_id, toStartOfDay(finish_ts), transaction_name, "
                        "cityHash64(transaction_span_id), op, cityHash64(trace_id), "
                        "cityHash64(span_id))"
                    ),
                    partition_by="(retention_days, toMonday(finish_ts))",
                    sample_by="cityHash64(span_id)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=TABLE_NAME_NEW,
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
            operations.SwapTables(
                storage_set=StorageSetKey.TRANSACTIONS,
                current_table_name=TABLE_NAME,
                new_table_name=TABLE_NAME_NEW,
                copy_order_by="toStartOfDay(finish_ts), project_id, trace_id, span_id",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RevertSwap(
                storage_set=StorageSetKey.TRANSACTIONS,
                current_table_name=TABLE_NAME,
                new_table_name=TABLE_NAME_NEW,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
