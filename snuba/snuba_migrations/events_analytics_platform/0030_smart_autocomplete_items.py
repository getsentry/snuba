from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations, table_engines
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
                    "arrayConcat(attributes_string, attributes_float)"
                )
            ),
        ),
    ),
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    # a hash of all the attribute keys of the item in sorted order
    # this lets us deduplicate rows with merges
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


_attr_num_names = ", ".join(
    [f"mapKeys(attributes_float_{i})" for i in range(num_attr_buckets)]
)
_attr_str_names = ", ".join(
    [f"mapKeys(attributes_string_{i})" for i in range(num_attr_buckets)]
)


MV_QUERY = f"""
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


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_item_co_occurring_attrs_1_local"
    dist_table_name = "eap_item_co_occurring_attrs_1_dist"
    mv_name = "eap_item_co_occurring_attrs_1_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        create_table_ops = [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.ReplacingMergeTree(
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
        materialized_view_ops: list[SqlOperation] = []

        materialized_view_ops.append(
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.mv_name,
                columns=columns,
                destination_table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
                query=MV_QUERY,
            ),
        )

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
                table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
