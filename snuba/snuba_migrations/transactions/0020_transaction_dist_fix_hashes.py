from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import CONTEXTS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration fixes the hash columns of the dist table to align with what is SaaS.
    In SaaS these columns do not have materialize modifiers. In this step we would drop those
    modifiers by dropping the columns and then adding them back without the modifier,
    however In Clickhouse 20, if we drop the materialized modifiers here then inserts will fail.
    So we leave them there for now and add the context_hash_map column.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "_contexts_hash_map",
                    Array(UInt(64), Modifiers(materialized=CONTEXTS_HASH_MAP_COLUMN)),
                ),
                after="contexts.value",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column_name="_contexts_hash_map",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]
