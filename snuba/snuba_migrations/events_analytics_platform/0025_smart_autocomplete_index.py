from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.utils.schemas import Array, Column, UInt


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_trace_item_attrs_local"
    dist_table_name = "eap_trace_item_attrs_dist"
    mv_name = "eap_trace_item_attrs_mv"

    str_hash_map_col = "_str_attr_keys_hash_map"
    float_hash_map_col = "_float64_attr_keys_hash_map"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # --- Str attrs -----
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    name=self.str_hash_map_col,
                    type=Array(
                        UInt(64),
                        Modifiers(
                            materialized=get_array_vals_hash("mapKeys(attrs_string)")
                        ),
                    ),
                ),
                after="attrs_string",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column(
                    self.str_hash_map_col,
                    type=Array(
                        UInt(64),
                        Modifiers(
                            materialized=get_array_vals_hash("mapKeys(attrs_string)")
                        ),
                    ),
                ),
                after="attrs_string",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name=f"bf_{self.str_hash_map_col}",
                index_expression=self.str_hash_map_col,
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            # --- Num attrs -----
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    name=self.str_hash_map_col,
                    type=Array(
                        UInt(64),
                        Modifiers(materialized=get_array_vals_hash("attrs_float64")),
                    ),
                ),
                after="attrs_float64",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column(
                    self.str_hash_map_col,
                    type=Array(
                        UInt(64),
                        Modifiers(materialized=get_array_vals_hash("attrs_float64")),
                    ),
                ),
                after="attrs_float64",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name=f"bf_{self.float_hash_map_col}",
                index_expression=self.float_hash_map_col,
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # --- Str attrs -----
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name=self.str_hash_map_col,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name=self.str_hash_map_col,
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name=f"bf_{self.str_hash_map_col}",
                target=operations.OperationTarget.LOCAL,
            ),
            # --- Num attrs -----
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name=self.str_hash_map_col,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name=self.str_hash_map_col,
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name=f"bf_{self.float_hash_map_col}",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
