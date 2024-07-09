from typing import Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import String


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds `resource_id`, `subtitle`, `culprit` and `level` columns.

    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        table_name = "search_issues_dist_v2"
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column=Column(
                    "resource_id",
                    String(Modifiers(nullable=True)),
                ),
                after="fingerprint",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column=Column(
                    "subtitle",
                    String(Modifiers(nullable=True)),
                ),
                after="search_title",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column=Column(
                    "culprit",
                    String(Modifiers(nullable=True)),
                ),
                after="subtitle",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column=Column(
                    "level",
                    String(Modifiers(low_cardinality=True, nullable=True)),
                ),
                after="culprit",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return []
        table_name = "search_issues_dist_v2"
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column_name="resource_id",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column_name="subtitle",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column_name="culprit",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES_TMP,
                table_name=table_name,
                column_name="level",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
