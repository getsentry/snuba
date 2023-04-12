from typing import Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column=Column(
                    "transaction_duration",
                    UInt(32),
                ),
                after="http_referer",
                target=params[1],
            )
            for params in [
                ("search_issues_local_v2", OperationTarget.LOCAL),
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column_name="transaction_duration",
                target=params[1],
            )
            for params in [
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
                ("search_issues_local_v2", OperationTarget.LOCAL),
            ]
        ]
