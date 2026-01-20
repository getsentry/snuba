from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Array, Column, Date, String, UInt

num_attr_buckets = 40

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column(
        "retention_days",
        UInt(16),
    ),
    Column(
        "attribute_keys_hash",
        Array(
            UInt(64),
            Modifiers(
                materialized=get_array_vals_hash(
                    "arrayDistinct(arrayConcat(attributes_string, attributes_float, attributes_bool))"
                )
            ),
        ),
    ),
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    Column("attributes_bool", Array(String())),
    # a hash of all the attribute keys of the item in sorted order
    # this lets us deduplicate rows with merges
    Column(
        "key_hash",
        UInt(
            64,
            Modifiers(
                materialized="cityHash64(arraySort(arrayDistinct(arrayConcat(attributes_string, attributes_float, attributes_bool))))"
            ),
        ),
    ),
]

_attr_num_names = ", ".join([f"mapKeys(attributes_float_{i})" for i in range(num_attr_buckets)])
_attr_str_names = ", ".join([f"mapKeys(attributes_string_{i})" for i in range(num_attr_buckets)])

MV_QUERY = f"""
SELECT
    organization_id AS organization_id,
    project_id AS project_id,
    item_type as item_type,
    toMonday(timestamp) AS date,
    retention_days as retention_days,
    arrayConcat({_attr_str_names}) AS attributes_string,
    mapKeys(attributes_bool) AS attributes_bool,
    arrayConcat({_attr_num_names}) AS attributes_float
FROM eap_items_1_local
"""


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    local_table_name = "eap_item_co_occurring_attrs_1_local"
    dist_table_name = "eap_item_co_occurring_attrs_1_dist"
    old_mv_name = "eap_item_co_occurring_attrs_1_mv"
    new_mv_name = "eap_item_co_occurring_attrs_2_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        # Add bool_attribute_keys column to local table
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column("attributes_bool", Array(String())),
                after="attributes_float",
                target=OperationTarget.LOCAL,
            )
        )

        # Add bool_attribute_keys column to distributed table
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column("attributes_bool", Array(String())),
                after="attributes_float",
                target=OperationTarget.DISTRIBUTED,
            )
        )

        # Create new MV with boolean keys
        ops.append(
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.new_mv_name,
                columns=columns,
                destination_table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
                query=MV_QUERY,
            )
        )

        # Drop old MV
        ops.append(
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.old_mv_name,
                target=OperationTarget.LOCAL,
            )
        )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        # Define the old MV query (without bool keys)
        old_mv_query = f"""
SELECT
    organization_id AS organization_id,
    project_id AS project_id,
    item_type as item_type,
    toMonday(timestamp) AS date,
    retention_days as retention_days,
    arrayConcat({_attr_str_names}) AS attributes_string,
    arrayConcat({_attr_num_names}) AS attributes_float
FROM eap_items_1_local
"""
        old_columns: List[Column[Modifiers]] = [
            Column("organization_id", UInt(64)),
            Column("project_id", UInt(64)),
            Column("item_type", UInt(8)),
            Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
            Column(
                "retention_days",
                UInt(16),
            ),
            Column(
                "attribute_keys_hash",
                Array(
                    UInt(64),
                    Modifiers(
                        materialized=get_array_vals_hash(
                            "arrayConcat(attributes_string, attributes_float)"
                        )
                    ),
                ),
            ),
            Column("attributes_string", Array(String())),
            Column("attributes_float", Array(String())),
            Column(
                "key_hash",
                UInt(
                    64,
                    Modifiers(
                        materialized="cityHash64(arraySort(arrayConcat(attributes_string, attributes_float)))"
                    ),
                ),
            ),
        ]

        ops: List[SqlOperation] = []

        # Recreate old MV
        ops.append(
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.old_mv_name,
                columns=old_columns,
                destination_table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
                query=old_mv_query,
            )
        )

        # Drop new MV
        ops.append(
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_mv_name,
                target=OperationTarget.LOCAL,
            )
        )

        # Drop bool_attribute_keys column from distributed table first
        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name="attributes_bool",
                target=OperationTarget.DISTRIBUTED,
            )
        )

        # Drop bool_attribute_keys column from local table
        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name="attributes_bool",
                target=OperationTarget.LOCAL,
            )
        )

        return ops
