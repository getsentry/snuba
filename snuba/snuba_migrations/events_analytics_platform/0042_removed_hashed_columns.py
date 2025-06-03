from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Array, Column, UInt

buckets = 40


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_items_1_local"
    dist_table_name = "eap_items_1_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        for target in [OperationTarget.DISTRIBUTED, OperationTarget.LOCAL]:
            for i in range(buckets):
                ops.append(
                    operations.DropColumn(
                        storage_set=self.storage_set_key,
                        table_name=self.local_table_name,
                        column_name=f"_hash_map_string_{i}",
                        target=target,
                    )
                )
                ops.append(
                    operations.DropColumn(
                        storage_set=self.storage_set_key,
                        table_name=self.local_table_name,
                        column_name=f"_hash_map_float_{i}",
                        target=target,
                    )
                )

            ops.append(
                operations.DropColumn(
                    storage_set=self.storage_set_key,
                    table_name=self.dist_table_name,
                    column_name="hashed_keys",
                    target=target,
                ),
            )

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        for target in [OperationTarget.LOCAL, OperationTarget.DISTRIBUTED]:
            for i in range(buckets):
                ops.append(
                    operations.AddColumn(
                        storage_set=self.storage_set_key,
                        table_name=self.local_table_name,
                        column=Column(
                            name=f"_hash_map_string_{i}",
                            type=Array(UInt(64)),
                        ),
                        target=target,
                    )
                )
                ops.append(
                    operations.AddColumn(
                        storage_set=self.storage_set_key,
                        table_name=self.local_table_name,
                        column=Column(
                            name=f"_hash_map_float_{i}",
                            type=Array(UInt(64)),
                        ),
                        target=target,
                    )
                )

            ops.append(
                operations.AddColumn(
                    storage_set=self.storage_set_key,
                    table_name=self.dist_table_name,
                    column=Column(
                        name="hashed_keys",
                        type=Array(UInt(64)),
                    ),
                    target=target,
                ),
            )

        return ops
