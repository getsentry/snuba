from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column=Column("profile_id", UUID(Modifiers(nullable=True))),
                after="transaction_duration",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column=Column("replay_id", UUID(Modifiers(nullable=True))),
                after="profile_id",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column_name="profile_id",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column_name="replay_id",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
