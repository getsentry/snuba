from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column=Column("occurrence_type_id", UInt(16)),
                target=params[1],
            )
            for params in [
                ("search_issues_local", OperationTarget.LOCAL),
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column=Column("occurrence_type_id", UInt(8)),
                target=params[1],
            )
            for params in [
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
                ("search_issues_local", OperationTarget.LOCAL),
            ]
        ]
