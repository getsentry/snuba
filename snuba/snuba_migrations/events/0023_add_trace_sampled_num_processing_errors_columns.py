from typing import Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            [
                operations.AddColumn(
                    storage_set=StorageSetKey.EVENTS,
                    table_name=table_name,
                    column=Column("trace_sampled", UInt(8, Modifiers(nullable=True))),
                    after="exception_main_thread",
                    target=target,
                ),
                operations.AddColumn(
                    storage_set=StorageSetKey.EVENTS,
                    table_name=table_name,
                    column=Column(
                        "num_processing_errors", UInt(64, Modifiers(nullable=True))
                    ),
                    after="trace_sampled",
                    target=target,
                ),
            ]
            for table_name, target in [
                ("errors_local", OperationTarget.LOCAL),
                ("errors_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

        return ops[0] + ops[1]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            [
                operations.DropColumn(
                    storage_set=StorageSetKey.EVENTS,
                    table_name=table_name,
                    column_name="trace_sampled",
                    target=target,
                ),
                operations.DropColumn(
                    storage_set=StorageSetKey.EVENTS,
                    table_name=table_name,
                    column_name="num_processing_errors",
                    target=target,
                ),
            ]
            for table_name, target in [
                ("errors_dist", OperationTarget.DISTRIBUTED),
                ("errors_local", OperationTarget.LOCAL),
            ]
        ]

        return ops[0] + ops[1]
