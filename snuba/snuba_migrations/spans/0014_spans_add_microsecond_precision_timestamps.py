from typing import List, Sequence

from snuba.clickhouse.columns import Column, DateTime64
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
table_prefix = "spans"
local_table_name = f"{table_prefix}_local"
dist_table_name = f"{table_prefix}_dist"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []
        for timestamp_column in {"start_timestamp", "end_timestamp"}:
            ops.extend(
                [
                    operations.AddColumn(
                        storage_set=storage_set_name,
                        table_name=local_table_name,
                        column=Column(
                            f"{timestamp_column}_precise",
                            DateTime64(
                                precision=6,
                                modifiers=Modifiers(codecs=["DoubleDelta"]),
                            ),
                        ),
                        target=OperationTarget.LOCAL,
                        after=timestamp_column,
                    ),
                    operations.AddColumn(
                        storage_set=storage_set_name,
                        table_name=dist_table_name,
                        column=Column(
                            f"{timestamp_column}_precise",
                            DateTime64(
                                precision=6,
                                modifiers=Modifiers(codecs=["DoubleDelta"]),
                            ),
                        ),
                        target=OperationTarget.DISTRIBUTED,
                        after=timestamp_column,
                    ),
                ]
            )
        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []
        for timestamp_column in {"start_timestamp", "end_timestamp"}:
            ops.extend(
                [
                    operations.DropColumn(
                        storage_set=storage_set_name,
                        table_name=dist_table_name,
                        column_name=f"{timestamp_column}_precise",
                        target=OperationTarget.DISTRIBUTED,
                    ),
                    operations.DropColumn(
                        storage_set=storage_set_name,
                        table_name=local_table_name,
                        column_name=f"{timestamp_column}_precise",
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )
        return ops
