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
        ops = [
            [
                operations.AddColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column=Column(
                        "resource_id",
                        String(Modifiers(nullable=True)),
                    ),
                    after="fingerprint",
                    target=params[1],
                ),
                operations.AddColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column=Column(
                        "subtitle",
                        String(Modifiers(nullable=True)),
                    ),
                    after="search_title",
                    target=params[1],
                ),
                operations.AddColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column=Column(
                        "culprit",
                        String(Modifiers(nullable=True)),
                    ),
                    after="subtitle",
                    target=params[1],
                ),
                operations.AddColumn(
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column=Column(
                        "level",
                        String(Modifiers(low_cardinality=True, nullable=True)),
                    ),
                    after="culprit",
                    target=params[1],
                ),
            ]
            for params in [
                ("search_issues_local_v2", OperationTarget.LOCAL),
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
            ]
        ]

        return ops[0] + ops[1]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            [
                operations.DropColumn(
                    StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column_name="resource_id",
                    target=params[1],
                ),
                operations.DropColumn(
                    StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column_name="subtitle",
                    target=params[1],
                ),
                operations.DropColumn(
                    StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column_name="culprit",
                    target=params[1],
                ),
                operations.DropColumn(
                    StorageSetKey.SEARCH_ISSUES,
                    table_name=params[0],
                    column_name="level",
                    target=params[1],
                ),
            ]
            for params in [
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
                ("search_issues_local_v2", OperationTarget.LOCAL),
            ]
        ]

        return ops[0] + ops[1]
