from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.utils.schemas import Column, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    local_table_name = "eap_spans_local"
    dist_table_name = "eap_spans_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=self.local_table_name,
                column=Column(
                    "sampling_weight_2",
                    UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"], default="1")),
                ),
                after="sampling_weight",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=self.dist_table_name,
                column=Column(
                    "sampling_weight_2",
                    UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
                after="sampling_weight",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=self.dist_table_name,
                column_name="sampling_weight_2",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=self.local_table_name,
                column_name="sampling_weight_2",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
