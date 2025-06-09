from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


TABLES = [
    ("dist", OperationTarget.DISTRIBUTED),
    ("local", OperationTarget.LOCAL),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    downsampled_factors = [8, 64, 512]
    buckets = range(5)
    table_name_prefix = "eap_items_1"

    def forwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = []
        for i in self.buckets:
            for ty in {"string", "float"}:
                for table_suffix, target in TABLES:
                    column_to_remove = hash_map_column_name(ty, i)
                    ops.extend(
                        [
                            operations.DropColumn(
                                storage_set=self.storage_set_key,
                                table_name=f"{self.table_name_prefix}_{table_suffix}",
                                column_name=column_to_remove,
                                target=target,
                            ),
                        ]
                    )
                    for factor in self.downsampled_factors:
                        downsampled_table_prefix = f"eap_items_1_downsample_{factor}"
                        ops.extend(
                            [
                                operations.DropColumn(
                                    storage_set=self.storage_set_key,
                                    table_name=f"{downsampled_table_prefix}_{table_suffix}",
                                    column_name=column_to_remove,
                                    target=target,
                                ),
                            ]
                        )
        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = []
        for i in self.buckets:
            for ty in {"string", "float"}:
                for table_suffix, target in reversed(TABLES):
                    column_to_remove = hash_map_column_name(ty, i)
                    ops.append(
                        operations.AddColumn(
                            storage_set=self.storage_set_key,
                            table_name=f"{self.table_name_prefix}_{table_suffix}",
                            column=Column(
                                column_to_remove,
                                Array(
                                    UInt(64),
                                    Modifiers(
                                        materialized=f"arrayMap(k -> cityHash64(k), mapKeys(attributes_{ty}_{i}))",
                                    ),
                                ),
                            ),
                            after=f"attributes_{ty}_{i}",
                            target=target,
                        )
                    )
                    for factor in self.downsampled_factors:
                        downsampled_table_prefix = f"eap_items_1_downsample_{factor}"
                        ops.append(
                            operations.AddColumn(
                                storage_set=self.storage_set_key,
                                table_name=f"{downsampled_table_prefix}_{table_suffix}",
                                column=Column(
                                    column_to_remove,
                                    Array(
                                        UInt(64),
                                        Modifiers(
                                            materialized=f"arrayMap(k -> cityHash64(k), mapKeys(attributes_{ty}_{i}))",
                                        ),
                                    ),
                                ),
                                after=f"attributes_{ty}_{i}",
                                target=target,
                            )
                        )
        return ops
