from typing import Sequence

from snuba.clickhouse.columns import UUID, Array, Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column("hierarchical_hashes", Array(UUID())),
                after="primary_hash",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column("level", String(Modifiers(low_cardinality=True, nullable=True))),
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS_RO,
                "errors_dist_ro",
                "_tags_hash_map",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                StorageSetKey.EVENTS_RO,
                "errors_dist_ro",
                "hierarchical_hashes",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                column=Column("level", String(Modifiers(nullable=True))),
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]
