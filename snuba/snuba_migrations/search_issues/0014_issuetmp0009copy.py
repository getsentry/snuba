from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column=Column("message", String()),
                after="replay_id",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column_name="message",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
