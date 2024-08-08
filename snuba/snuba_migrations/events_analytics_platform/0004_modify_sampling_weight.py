from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.utils.schemas import Column, Float, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                column=Column(
                    "sampling_weight", UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))
                ),
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                column=Column(
                    "sampling_weight",
                    Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"])),
                ),
            )
        ]
