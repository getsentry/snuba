from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    downsampled_factors = [8, 64, 512]
    buckets = range(5)

    def forwards_ops(self) -> list[operations.SqlOperation]:
        table_name_prefix = "eap_items_1"
        ops: list[operations.SqlOperation] = []
        for i in self.buckets:
            for ty in {"string", "float"}:
                column_to_remove = hash_map_column_name(ty, i)
                ops.extend(
                    [
                        operations.DropColumn(
                            storage_set=self.storage_set_key,
                            table_name=f"{table_name_prefix}_dist",
                            column_name=column_to_remove,
                            target=OperationTarget.DISTRIBUTED,
                        ),
                        operations.DropColumn(
                            storage_set=self.storage_set_key,
                            table_name=f"{table_name_prefix}_local",
                            column_name=self.column_to_remove,
                            target=OperationTarget.LOCAL,
                        ),
                    ]
                )
                for factor in self.downsampled_factors:
                    downsampled_table_prefix = f"eap_items_1_downsample_{factor}"
                    ops.extend(
                        [
                            operations.DropColumn(
                                storage_set=self.storage_set_key,
                                table_name=f"{downsampled_table_prefix}_dist",
                                column_name=column_to_remove,
                                target=OperationTarget.DISTRIBUTED,
                            ),
                            operations.DropColumn(
                                storage_set=self.storage_set_key,
                                table_name=f"{downsampled_table_prefix}_local",
                                column_name=column_to_remove,
                                target=OperationTarget.LOCAL,
                            ),
                        ]
                    )
        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return []
