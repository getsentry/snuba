from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    Copy of 0004_transactions_add_tags_hash_map.py

    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
                column_name="_tags_hash_map",
                target=OperationTarget.LOCAL,
            ),
        ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
                column_name="_tags_hash_map",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
