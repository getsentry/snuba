from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import AddIndicesData, OperationTarget, SqlOperation
from snuba.utils.schemas import (
    UUID,
    Column,
    DateTime,
    DateTime64,
    Float,
    Int,
    Map,
    String,
    UInt,
)

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_spans_2_local"
dist_table_name = "eap_spans_2_dist"
num_attr_buckets = 20

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("service", String(Modifiers(codecs=["ZSTD(1)"]))),
    Column("trace_id", UUID()),
    Column("span_id", UInt(64)),
    Column("parent_span_id", UInt(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("segment_id", UInt(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("segment_name", String(Modifiers(codecs=["ZSTD(1)"]))),
    Column("is_segment", UInt(8, Modifiers(codecs=["LZ4"]))),
    Column("_sort_timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column(
        "start_timestamp",
        DateTime64(6, modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
    ),
    Column(
        "end_timestamp",
        DateTime64(6, modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
    ),
    Column(
        "duration_micro",
        UInt(64, modifiers=Modifiers(codecs=["T64", "ZSTD(1)"])),
    ),
    Column(
        "exclusive_time_micro",
        UInt(64, modifiers=Modifiers(codecs=["T64", "ZSTD(1)"])),
    ),
    Column(
        "retention_days",
        UInt(16, modifiers=Modifiers(codecs=["T64", "ZSTD(1)"])),
    ),
    Column("name", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sampling_weight", UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sign", Int(8, modifiers=Modifiers(codecs=["LZ4"]))),
]

columns.extend(
    [
        Column(
            f"attr_str_{i}",
            Map(String(), String(), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(num_attr_buckets)
    ]
)
columns.extend(
    [
        Column(
            f"attr_num_{i}",
            Map(String(), Float(64), modifiers=Modifiers(codecs=["ZSTD(1)"])),
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
                engine=table_engines.CollapsingMergeTree(
                    order_by="(organization_id, _sort_timestamp, trace_id, span_id)",
                    partition_by="(retention_days, toMonday(_sort_timestamp))",
                    primary_key="(organization_id, _sort_timestamp, trace_id)",
                    settings={"index_granularity": "8192"},
                    sign_column="sign",
                    storage_set=storage_set_name,
                    ttl="_sort_timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="cityHash64(reinterpretAsUInt128(trace_id))",  # sharding keys must be at most 64 bits
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
