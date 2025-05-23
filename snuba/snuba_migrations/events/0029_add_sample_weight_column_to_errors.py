from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import (
    AddColumn,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.utils.schemas import Column, Float


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column(
                    "sample_weight",
                    Float(64, modifiers=MigrationModifiers(nullable=True)),
                ),
                after="timestamp_ms",
                target=OperationTarget.LOCAL,
            ),
            AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column(
                    "sample_weight",
                    Float(64, modifiers=MigrationModifiers(nullable=True)),
                ),
                after="timestamp_ms",
                target=OperationTarget.DISTRIBUTED,
            ),
            AddColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column(
                    "sample_weight",
                    Float(64, modifiers=MigrationModifiers(nullable=True)),
                ),
                after="timestamp_ms",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column_name="sample_weight",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column_name="sample_weight",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column_name="sample_weight",
                target=OperationTarget.LOCAL,
            ),
        ]
