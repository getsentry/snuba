from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


class Migration(migration.ClickhouseNodeMigration):
    """
    Add metrics_summary column.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "metrics_summary",
                    String(
                        Modifiers(
                            default="''",
                            codecs=["ZSTD(1)"],
                        )
                    ),
                ),
                target=OperationTarget.LOCAL,
                after="measurements",
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "metrics_summary",
                    String(
                        Modifiers(
                            default="''",
                            codecs=["ZSTD(1)"],
                        )
                    ),
                ),
                target=OperationTarget.DISTRIBUTED,
                after="measurements",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="metrics_summary",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="metrics_summary",
                target=OperationTarget.LOCAL,
            ),
        ]
