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
    sampling_weights = [8, 8**2, 8**3]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "sampling_factor",
                    Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
                after="sampling_weight",
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "sampling_factor",
                    Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
                after="sampling_weight",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

        for sampling_weight in self.sampling_weights:
            downsampled_local_table_name = (
                f"eap_items_1_downsample_{sampling_weight}_local"
            )
            ops.append(
                operations.AddColumn(
                    storage_set=storage_set_name,
                    table_name=downsampled_local_table_name,
                    column=Column(
                        "sampling_factor",
                        Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                    ),
                    after="sampling_weight",
                    target=OperationTarget.LOCAL,
                )
            )

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="sampling_factor",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="sampling_factor",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

        for sampling_weight in self.sampling_weights:
            downsampled_local_table_name = (
                f"eap_items_1_downsample_{sampling_weight}_local"
            )
            ops.append(
                operations.DropColumn(
                    storage_set=storage_set_name,
                    table_name=downsampled_local_table_name,
                    column_name="sampling_factor",
                    target=OperationTarget.LOCAL,
                )
            )

        return ops
