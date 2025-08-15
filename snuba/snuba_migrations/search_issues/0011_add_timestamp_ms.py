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
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local_v2",
                column=Column(
                    "timestamp_ms",
                    schemas.DateTime64(
                        3,
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
                after="group_first_seen",
                target=OperationTarget.LOCAL,
            ),
            AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist_v2",
                column=Column(
                    "timestamp_ms",
                    schemas.DateTime64(
                        3,
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
                after="group_first_seen",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist_v2",
                column_name="timestamp_ms",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local_v2",
                column_name="timestamp_ms",
                target=OperationTarget.LOCAL,
            ),
        ]
