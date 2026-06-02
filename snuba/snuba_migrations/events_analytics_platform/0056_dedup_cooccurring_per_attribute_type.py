from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Array, Column, Date, String, UInt

num_attr_buckets = 40

# preserves per-type structure so e.g. {str=[a,b], bool=[c]} and {str=[a],
# bool=[b,c]} don't collide
KEY_HASH_EXPR = (
    "cityHash64(("
    "arraySort(attributes_string),"
    "arraySort(attributes_float),"
    "arraySort(attributes_bool),"
    "arraySort(attributes_int),"
    "arraySort(attributes_array)"
    "))"
)

# Per-type tag prepended to each key before hashing so that 'foo' stored as
# a string and 'foo' stored as a bool produce distinct entries in the bloom
# filter. The read path must use the same mapping when constructing the
# hasAll predicate.
#   1 = string, 2 = float, 3 = bool, 4 = int, 5 = array
ATTRIBUTE_KEYS_HASH_EXPR = (
    "arrayConcat("
    "arrayMap(k -> cityHash64((1, k)), attributes_string),"
    "arrayMap(k -> cityHash64((2, k)), attributes_float),"
    "arrayMap(k -> cityHash64((3, k)), attributes_bool),"
    "arrayMap(k -> cityHash64((4, k)), attributes_int),"
    "arrayMap(k -> cityHash64((5, k)), attributes_array)"
    ")"
)

_attr_str_names = ", ".join([f"mapKeys(attributes_string_{i})" for i in range(num_attr_buckets)])
_attr_num_names = ", ".join([f"mapKeys(attributes_float_{i})" for i in range(num_attr_buckets)])

MV_QUERY = f"""
SELECT
    organization_id AS organization_id,
    project_id AS project_id,
    item_type AS item_type,
    toMonday(timestamp) AS date,
    retention_days AS retention_days,
    arraySort(arrayConcat({_attr_str_names})) AS attributes_string,
    arraySort(arrayConcat({_attr_num_names})) AS attributes_float,
    arraySort(mapKeys(attributes_bool)) AS attributes_bool,
    arraySort(mapKeys(attributes_int)) AS attributes_int,
    arraySort(JSONExtractKeys(attributes_array)) AS attributes_array
FROM eap_items_1_local
"""

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("retention_days", UInt(16)),
    Column(
        "attribute_keys_hash",
        Array(
            UInt(64),
            Modifiers(materialized=ATTRIBUTE_KEYS_HASH_EXPR),
        ),
    ),
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    Column("attributes_bool", Array(String())),
    Column("attributes_int", Array(String())),
    Column("attributes_array", Array(String())),
    Column("key_hash", UInt(64, Modifiers(default=KEY_HASH_EXPR))),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    local_table_name = "eap_item_co_occurring_attrs_1_local"
    dist_table_name = "eap_item_co_occurring_attrs_1_dist"
    old_mv_name = "eap_item_co_occurring_attrs_2_mv"
    new_mv_name = "eap_item_co_occurring_attrs_3_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        # add attributes_int column (local + dist)
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column("attributes_int", Array(String())),
                after="attributes_bool",
                target=OperationTarget.LOCAL,
            )
        )
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column("attributes_int", Array(String())),
                after="attributes_bool",
                target=OperationTarget.DISTRIBUTED,
            )
        )

        # add attributes_array column (local + dist)
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column("attributes_array", Array(String())),
                after="attributes_int",
                target=OperationTarget.LOCAL,
            )
        )
        ops.append(
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column("attributes_array", Array(String())),
                after="attributes_int",
                target=OperationTarget.DISTRIBUTED,
            )
        )

        # extend attribute_keys_hash to cover all five attribute types
        # with per-type tagging — so e.g. cityHash64((1, 'foo')) is a hit
        # only for items where 'foo' is a string attribute
        ops.append(
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    "attribute_keys_hash",
                    Array(
                        UInt(64),
                        Modifiers(materialized=ATTRIBUTE_KEYS_HASH_EXPR),
                    ),
                ),
                target=OperationTarget.LOCAL,
            )
        )

        # switch key_hash to a tuple-of-sorted-arrays hash so rows that
        # differ only in a single attribute type get distinct hashes
        ops.append(
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column("key_hash", UInt(64, Modifiers(default=KEY_HASH_EXPR))),
                target=OperationTarget.LOCAL,
            )
        )

        # bring up the new MV before tearing the old one down to avoid an
        # ingestion gap; old MV inserts will leave the new columns empty,
        # which is fine — TTL drops the overlap window
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

        ops.append(
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.old_mv_name,
                target=OperationTarget.LOCAL,
            )
        )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        old_mv_query = f"""
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

        old_columns: List[Column[Modifiers]] = [
            Column("organization_id", UInt(64)),
            Column("project_id", UInt(64)),
            Column("item_type", UInt(8)),
            Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
            Column("retention_days", UInt(16)),
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
            Column(
                "key_hash",
                UInt(
                    64,
                    Modifiers(
                        default="cityHash64(arraySort(arrayConcat(attributes_string, attributes_float, attributes_bool)))"
                    ),
                ),
            ),
        ]

        ops: List[SqlOperation] = []

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

        ops.append(
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_mv_name,
                target=OperationTarget.LOCAL,
            )
        )

        # revert key_hash to the 0054 expression
        ops.append(
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    "key_hash",
                    UInt(
                        64,
                        Modifiers(
                            default="cityHash64(arraySort(arrayConcat(attributes_string, attributes_float, attributes_bool)))"
                        ),
                    ),
                ),
                target=OperationTarget.LOCAL,
            )
        )

        # revert attribute_keys_hash to the 0055 expression
        ops.append(
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
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
                target=OperationTarget.LOCAL,
            )
        )

        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name="attributes_array",
                target=OperationTarget.DISTRIBUTED,
            )
        )
        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name="attributes_array",
                target=OperationTarget.LOCAL,
            )
        )

        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name="attributes_int",
                target=OperationTarget.DISTRIBUTED,
            )
        )
        ops.append(
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name="attributes_int",
                target=OperationTarget.LOCAL,
            )
        )

        return ops
