from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    Heavily influenced by 0004_transactions_add_tags_hash_map.py

    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=params[1],
            )
            for params in [
                ("search_issues_local", OperationTarget.LOCAL),
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                column_name="_tags_hash_map",
                target=params[1],
            )
            for params in [
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
                ("search_issues_local", OperationTarget.LOCAL),
            ]
        ]
