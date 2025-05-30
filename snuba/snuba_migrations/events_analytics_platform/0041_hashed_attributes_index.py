from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Array, Column, UInt

buckets = 40


def get_hashed_attributes_column_expression() -> str:
    column_expressions = []
    for i in range(buckets):
        hashed_keys_string = f"arrayMap(kv -> cityHash64(concat(kv.1, '=', kv.2)), arrayZip(mapKeys(attributes_string_{i}), mapValues(attributes_string_{i})))"
        hashed_keys_float = f"arrayMap(kv -> cityHash64(concat(kv.1, '=', kv.2)), arrayZip(mapKeys(attributes_float_{i}), mapValues(attributes_float_{i})))"
        column_expressions.append(hashed_keys_string)
        column_expressions.append(hashed_keys_float)

    return f"arrayConcat({', '.join(column_expressions)})"


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_items_1_local"
    dist_table_name = "eap_items_1_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    "hashed_attributes",
                    Array(UInt(64)),
                ),
                after="attributes_float_39",
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column(
                    "hashed_attributes",
                    Array(UInt(64)),
                ),
                after="attributes_float_39",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_hashed_attributes",
                index_expression="hashed_attributes",
                index_type="bloom_filter",
                granularity=1,
                target=OperationTarget.LOCAL,
            ),
        ]

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = [
            operations.DropIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_hashed_attributes",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name="hashed_attributes",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name="hashed_attributes",
                target=OperationTarget.LOCAL,
            ),
        ]

        return ops
