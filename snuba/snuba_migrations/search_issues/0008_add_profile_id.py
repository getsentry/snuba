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
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=table_name,
                column=Column("profile_id", UUID(Modifiers(nullable=True))),
                after="transaction_duration",
                target=target,
            )
            for table_name, target in [
                ("search_issues_local_v2", OperationTarget.LOCAL),
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=table_name,
                column_name="profile_id",
                target=target,
            )
            for table_name, target in [
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
                ("search_issues_local_v2", OperationTarget.LOCAL),
            ]
        ]
