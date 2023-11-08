from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import (
    hash_map_int_column_definition,
    hash_map_int_key_str_value_column_definition,
)
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False
    dist_table_name = "generic_metric_gauges_aggregated_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_GAUGES

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
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
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column(
                    "_raw_tags_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=hash_map_int_key_str_value_column_definition(
                                "tags.key", "tags.raw_value"
                            )
                        ),
                    ),
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                column_name="_raw_tags_hash",
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
            ),
            operations.DropColumn(
                column_name="_indexed_tags_hash",
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
            ),
        ]
