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
from snuba.utils import schemas
from snuba.utils.schemas import Column


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            AddColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column(
                    "symbolicated_in_app",
                    schemas.UInt(
                        8,
                        modifiers=MigrationModifiers(
                            nullable=True,
                            low_cardinality=False,
                            default=None,
                            materialized=None,
                            codecs=None,
                            ttl=None,
                        ),
                    ),
                ),
                after="replay_id",
                target=OperationTarget.DISTRIBUTED,
            )
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column_name="symbolicated_in_app",
                target=OperationTarget.DISTRIBUTED,
            )
        ]
