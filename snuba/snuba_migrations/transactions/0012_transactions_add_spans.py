from typing import Sequence

from snuba.clickhouse.columns import Array, Column, Float, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigrationLegacy):

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "spans",
                    Nested(
                        [
                            ("op", String(Modifiers(low_cardinality=True))),
                            ("group", UInt(64)),
                            ("exclusive_time", Float(64)),
                        ]
                    ),
                ),
                after="span_op_breakdowns.value",
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("spans.op", Array(String(Modifiers(low_cardinality=True)))),
                ttl_month=("finish_ts", 1),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("spans.group", Array(UInt(64))),
                ttl_month=("finish_ts", 1),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("spans.exclusive_time", Array(Float(64))),
                ttl_month=("finish_ts", 1),
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="bf_spans_op",
                index_expression="spans.op",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="bf_spans_group",
                index_expression="spans.group",
                index_type="bloom_filter()",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="bf_spans_group",
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="bf_spans_op",
            ),
            operations.DropColumn(StorageSetKey.TRANSACTIONS, "transactions_local", "spans"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "spans",
                    Nested(
                        [
                            ("op", String(Modifiers(low_cardinality=True))),
                            ("group", UInt(64)),
                            ("exclusive_time", Float(64)),
                        ]
                    ),
                ),
                after="span_op_breakdowns.value",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [operations.DropColumn(StorageSetKey.TRANSACTIONS, "transactions_dist", "spans")]
