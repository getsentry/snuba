from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_items_1_local"
dist_table_name = "eap_items_1_dist"
num_attr_buckets = 40


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column=Column(
                    hash_map_column_name(attribute_type, i),
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=f"arrayMap(k -> cityHash64(k), mapKeys(attributes_{attribute_type}_{i}))",
                        ),
                    ),
                ),
                after=f"attributes_{attribute_type}_{i}",
                target=target,
            )
            for i in range(num_attr_buckets)
            for attribute_type in {"string", "float"}
            for (table_name, target) in [
                (local_table_name, OperationTarget.LOCAL),
                (dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column_name=hash_map_column_name(attribute_type, i),
                target=target,
            )
            for i in range(num_attr_buckets)
            for attribute_type in {"string", "float"}
            for (table_name, target) in [
                (dist_table_name, OperationTarget.DISTRIBUTED),
                (local_table_name, OperationTarget.LOCAL),
            ]
        ]
