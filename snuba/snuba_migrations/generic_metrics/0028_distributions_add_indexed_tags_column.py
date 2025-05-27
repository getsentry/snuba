from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import hash_map_int_column_definition
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    dist_table_name = "generic_metric_distributions_aggregated_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column(
                    "_indexed_tags_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=hash_map_int_column_definition(
                                "tags.key", "tags.indexed_value"
                            )
                        ),
                    ),
                ),
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                column_name="_indexed_tags_hash",
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]
