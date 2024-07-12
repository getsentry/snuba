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
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column=Column(
                    "transaction_duration",
                    UInt(32),
                ),
                after="http_referer",
                target=OperationTarget.DISTRIBUTED,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return []
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES_TMP,
                table_name="search_issues_dist_v2",
                column_name="transaction_duration",
                target=OperationTarget.DISTRIBUTED,
            )
        ]
