from collections.abc import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import (
    Array,
    Column,
    Date,
    DateTime,
    SimpleAggregateFunction,
    String,
    UInt,
)

num_attr_buckets = 40

# Every attribute-key array in the table, across all attribute types (the scalar
# string/float/int/bool maps and the four array-valued maps). The dedup `key_hash`
# and the bloom-filter `attribute_keys_hash` are both derived from this so they
# cover every attribute key regardless of its type.
_all_attribute_keys = (
    "arrayConcat("
    "attributes_string, attributes_float, attributes_int, attributes_bool, "
    "attributes_array_string, attributes_array_int, "
    "attributes_array_float, attributes_array_bool)"
)

columns: list[Column[Modifiers]] = [
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
            Modifiers(materialized=get_array_vals_hash(f"arrayDistinct({_all_attribute_keys})")),
        ),
    ),
    # one array of attribute keys per attribute type, mirroring the typed maps on
    # eap_items so every attribute can be surfaced with its type.
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    Column("attributes_int", Array(String())),
    Column("attributes_bool", Array(String())),
    # keys of the array-valued attributes, split by element type to match the
    # AttributeKey TYPE_ARRAY_STRING / TYPE_ARRAY_INT / TYPE_ARRAY_DOUBLE /
    # TYPE_ARRAY_BOOL types (float arrays map to TYPE_ARRAY_DOUBLE).
    Column("attributes_array_string", Array(String())),
    Column("attributes_array_int", Array(String())),
    Column("attributes_array_float", Array(String())),
    Column("attributes_array_bool", Array(String())),
    # a hash of all the attribute keys of the item in sorted order
    # this lets us deduplicate rows with merges
    Column(
        "key_hash",
        UInt(
            64,
            Modifiers(materialized=f"cityHash64(arraySort(arrayDistinct({_all_attribute_keys})))"),
        ),
    ),
    Column("count", UInt(64)),
    # the most recent timestamp at which this set of attributes was seen.
    # SummingMergeTree applies the `max` aggregate function on merge, so this
    # keeps the latest timestamp across all the rows that get collapsed.
    Column("last_seen", SimpleAggregateFunction("max", [DateTime()])),
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
    arrayConcat({_attr_num_names}) AS attributes_float,
    mapKeys(attributes_int) AS attributes_int,
    mapKeys(attributes_bool) AS attributes_bool,
    mapKeys(attributes_array_string) AS attributes_array_string,
    mapKeys(attributes_array_int) AS attributes_array_int,
    mapKeys(attributes_array_float) AS attributes_array_float,
    mapKeys(attributes_array_bool) AS attributes_array_bool,
    1 AS count,
    timestamp AS last_seen
FROM eap_items_1_local
"""


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    local_table_name = "eap_item_co_occurring_attrs_2_local"
    dist_table_name = "eap_item_co_occurring_attrs_2_dist"
    mv_name = "eap_item_co_occurring_attrs_3_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        create_table_ops = [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.SummingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, project_id, date, item_type, key_hash)",
                    order_by="(organization_id, project_id, date, item_type, key_hash, retention_days)",
                    partition_by="(retention_days, toMonday(date))",
                    ttl="date + toIntervalDay(retention_days)",
                ),
                columns=columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name,
                    sharding_key=None,
                ),
                columns=columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

        index_ops = [
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_attribute_keys_hash",
                index_expression="attribute_keys_hash",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

        materialized_view_ops: list[SqlOperation] = [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.mv_name,
                columns=columns,
                destination_table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
                query=MV_QUERY,
            ),
        ]

        return create_table_ops + index_ops + materialized_view_ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.mv_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
