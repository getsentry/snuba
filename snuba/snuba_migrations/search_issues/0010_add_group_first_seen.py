from typing import Sequence

from snuba.migrations.columns import MigrationModifiers
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Column, DateTime


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            [
                operations.AddColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=table_name,
                    column=Column(
                        "group_first_seen", DateTime(MigrationModifiers(nullable=True))
                    ),
                    after="message",
                    target=target,
                ),
            ]
            for table_name, target in [
                ("search_issues_local_v2", OperationTarget.LOCAL),
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
            ]
        ]
        return ops[0] + ops[1]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            [
                operations.DropColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=table_name,
                    column_name="group_first_seen",
                    target=target,
                ),
            ]
            for table_name, target in [
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
                ("search_issues_local_v2", OperationTarget.LOCAL),
            ]
        ]
        return ops[0] + ops[1]
