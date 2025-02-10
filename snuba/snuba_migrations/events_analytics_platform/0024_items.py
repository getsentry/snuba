from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import AddIndicesData, OperationTarget, SqlOperation
from snuba.utils.schemas import (
    UUID,
    Bool,
    Column,
    DateTime,
    Float,
    Int,
    Map,
    String,
    UInt,
)

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_items_1_local"
dist_table_name = "eap_items_1_dist"
num_attr_buckets = 40

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("trace_id", UUID()),
    Column("item_id", UInt(128)),
    Column("sampling_weight", UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column(
        "retention_days",
        UInt(16, modifiers=Modifiers(codecs=["T64", "ZSTD(1)"])),
    ),
    Column(
        "attributes_bool",
        Map(
            String(),
            Bool(),
        ),
    ),
    Column(
        "attributes_int64",
        Map(
            String(),
            Int(64),
        ),
    ),
]

columns.extend(
    [
        Column(
            f"attributes_string_{i}",
            Map(
                String(),
                String(),
                modifiers=Modifiers(
                    codecs=["ZSTD(1)"],
                ),
            ),
        )
        for i in range(num_attr_buckets)
    ]
)

columns.extend(
    [
        Column(
            f"attributes_float64_{i}",
            Map(
                String(),
                Float(64),
                modifiers=Modifiers(
                    codecs=["ZSTD(1)"],
                ),
            ),
        )
        for i in range(num_attr_buckets)
    ]
)


indices: Sequence[AddIndicesData] = [
    AddIndicesData(
        name="bf_trace_id",
        expression="trace_id",
        type="bloom_filter",
        granularity=1,
    )
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        res: List[SqlOperation] = [
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    primary_key="(organization_id, project_id, item_type, timestamp, trace_id)",
                    order_by="(organization_id, project_id, item_type, timestamp, trace_id, item_id)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set_name,
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="cityHash64(reinterpretAsUInt128(trace_id))",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddIndices(
                storage_set=storage_set_name,
                table_name=local_table_name,
                indices=indices,
                target=OperationTarget.LOCAL,
            ),
        ]
        return res

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
