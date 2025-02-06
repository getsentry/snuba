from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Column, Date, Float, Int, Map, String, UInt

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_items_1_local"
dist_table_name = "eap_items_1_dist"
num_attr_buckets = 20

_TYPES = {
    "string": String(),
    "bool": Int(8),
    "int64": Int(64),
    "float64": Float(64),
}


_attr_columns = [
    Column(f"attrs_{type_name}", Map(String(), type_spec))
    for type_name, type_spec in _TYPES.items()
]


columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("item_type", String()),
    Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("retention_days", UInt(16)),
    *_attr_columns,
    Column("key_val_hash", UInt(64)),
]


MV_QUERY = """
SELECT
    project_id,
    'span',
    toDate(_sort_timestamp) AS date,
    retention_days as retention_days,
    mapConcat(attr_str_0, attr_str_1, attr_str_2, attr_str_3, attr_str_4, attr_str_5, attr_str_6, attr_str_7, attr_str_8, attr_str_9, attr_str_10, attr_str_11, attr_str_12, attr_str_13, attr_str_14, attr_str_15, attr_str_16, attr_str_17, attr_str_18, attr_str_19) AS attrs_string, -- `attrs_string` Map(String, String),
    map() AS attrs_bool, -- bool
    map() AS attrs_int64, -- int64
    mapConcat(attr_num_0, attr_num_1, attr_num_2, attr_num_3, attr_num_4, attr_num_5, attr_num_6, attr_num_7, attr_num_8, attr_num_9, attr_num_10, attr_num_11, attr_num_12, attr_num_13, attr_num_14, attr_num_15, attr_num_16, attr_num_17, attr_num_18, attr_num_19) AS attrs_float64, -- float
    -- a hash of all the attribute key,val pairs of the item in sorted order
    -- this lets us deduplicate rows with merges
    cityHash64(mapSort(
        mapConcat(
            mapApply((k, v) -> (k, ''), attr_num_0),
            mapApply((k, v) -> (k, ''), attr_num_1),
            mapApply((k, v) -> (k, ''), attr_num_2),
            mapApply((k, v) -> (k, ''), attr_num_3),
            mapApply((k, v) -> (k, ''), attr_num_4),
            mapApply((k, v) -> (k, ''), attr_num_5),
            mapApply((k, v) -> (k, ''), attr_num_6),
            mapApply((k, v) -> (k, ''), attr_num_7),
            mapApply((k, v) -> (k, ''), attr_num_8),
            mapApply((k, v) -> (k, ''), attr_num_9),
            mapApply((k, v) -> (k, ''), attr_num_10),
            mapApply((k, v) -> (k, ''), attr_num_11),
            mapApply((k, v) -> (k, ''), attr_num_12),
            mapApply((k, v) -> (k, ''), attr_num_13),
            mapApply((k, v) -> (k, ''), attr_num_14),
            mapApply((k, v) -> (k, ''), attr_num_15),
            mapApply((k, v) -> (k, ''), attr_num_16),
            mapApply((k, v) -> (k, ''), attr_num_17),
            mapApply((k, v) -> (k, ''), attr_num_18),
            mapApply((k, v) -> (k, ''), attr_num_19),
            attr_str_0,
            attr_str_1,
            attr_str_2,
            attr_str_3,
            attr_str_4,
            attr_str_5,
            attr_str_6,
            attr_str_7,
            attr_str_8,
            attr_str_9,
            attr_str_10,
            attr_str_11,
            attr_str_12,
            attr_str_13,
            attr_str_14,
            attr_str_15,
            attr_str_16,
            attr_str_17,
            attr_str_18,
            attr_str_19
        )
    )) AS key_val_hash
FROM eap_spans_2_local


"""


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_trace_item_attrs_local"
    dist_table_name = "eap_trace_item_attrs_dist"
    mv_name = "eap_trace_item_attrs_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        create_table_ops = [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(project_id, date, key_val_hash)",
                    order_by="(project_id, date, key_val_hash)",
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

        return create_table_ops + materialized_view_ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
