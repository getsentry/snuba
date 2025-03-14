from typing import Sequence

from snuba.clickhouse.columns import Column, Float
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_items_1_local"
dist_table_name = "eap_items_1_dist"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "sampling_weight_2",
                    Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
                after="sampling_weight",
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "sampling_weight_2",
                    Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
                after="sampling_weight",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="sampling_weight_2",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="sampling_weight_2",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
