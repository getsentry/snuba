from typing import List, Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import CONTEXTS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

indexes = [
    ("minmax_transaction_name", "transaction_name", "minmax", 1),
    ("minmax_transaction_status", "transaction_status", "minmax", 1),
    ("minmax_transaction_op", "transaction_op", "minmax", 1),
    ("minmax_start_ts", "start_ts", "minmax", 1),
    ("minmax_duration", "duration", "minmax", 1),
    ("minmax_event_id", "event_id", "minmax", 1),
    ("minmax_span_id", "span_id", "minmax", 1),
    ("minmax_release", "release", "minmax", 1),
    ("minmax_dist", "dist", "minmax", 1),
    ("minmax_environment", "environment", "minmax", 1),
    ("bf_tags_key", "tags.key", "bloom_filter", 1),
    ("bf_contexts_key", "contexts.key", "bloom_filter", 1),
    ("bf_tags_hash_map", "_tags_hash_map", "bloom_filter", 1),
    ("bf_tags_hash_map_no_fp", "_tags_hash_map", "bloom_filter(0.)", 1),
    ("bf_measurements_key", "measurements.key", "bloom_filter", 1),
    ("bf_span_op_breakdowns_key", "span_op_breakdowns.key", "bloom_filter", 1),
    ("bf_contexts_hash_map", "_contexts_hash_map", "bloom_filter", 1),
    ("bf_spans_op", "spans.op", "bloom_filter", 1),
    ("bf_spans_group", "spans.group", "bloom_filter", 1),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    This migrations adds indexes to the transactions table table to match SaaS.
    Additionally, it adds the _contexts_hash_map column to match SaaS also.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "_contexts_hash_map",
                    Array(UInt(64), Modifiers(materialized=CONTEXTS_HASH_MAP_COLUMN)),
                ),
                after="contexts.value",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "_contexts_hash_map",
                    Array(UInt(64), Modifiers(materialized=CONTEXTS_HASH_MAP_COLUMN)),
                ),
                after="contexts.value",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

        # apply the indexes after the minmax_timestamp index and have them follow each other
        # one after the other
        index_names = [index[0] for index in indexes]
        for index, after_col in zip(indexes, ["minmax_timestamp"] + index_names[:-1]):
            index_name, index_expression, index_type, granularity = index
            ops.append(
                operations.AddIndex(
                    StorageSetKey.TRANSACTIONS,
                    "transactions_local",
                    index_name,
                    index_expression,
                    index_type,
                    granularity,
                    after=after_col if after_col else None,
                    target=operations.OperationTarget.LOCAL,
                )
            )

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []

        for index in indexes:
            index_name, _, _, _ = index
            ops.append(
                operations.DropIndex(
                    StorageSetKey.TRANSACTIONS,
                    "transactions_local",
                    index_name,
                    target=operations.OperationTarget.LOCAL,
                )
            )

        ops.append(
            operations.DropColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column_name="_contexts_hash_map",
                target=operations.OperationTarget.DISTRIBUTED,
            )
        )
        ops.append(
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS,
                "transactions_local",
                "_contexts_hash_map",
                target=operations.OperationTarget.LOCAL,
            ),
        )

        return ops
