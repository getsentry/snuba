from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import (
    AddColumn,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.utils import schemas
from snuba.utils.schemas import Column


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("quantity_64", schemas.UInt(64, modifiers=None)),
                after="quantity",
                target=OperationTarget.LOCAL,
            ),
            AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column=Column("quantity_64", schemas.UInt(64, modifiers=None)),
                after="quantity",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column_name="quantity_64",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column_name="quantity_64",
                target=OperationTarget.LOCAL,
            ),
        ]
