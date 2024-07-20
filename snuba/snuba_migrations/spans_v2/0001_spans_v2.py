from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import (
    UUID,
    Bool,
    Column,
    DateTime,
    DateTime64,
    Float,
    Int,
    Map,
    String,
    UInt,
)

storage_set_name = StorageSetKey.SPANS_V2
local_table_name = "spans_v2_local"
dist_table_name = "spans_v2_dist"

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("trace_id", UUID()),
    Column("span_id", UInt(64)),
    Column("parent_span_id", UInt(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("segment_id", UInt(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("segment_name", String(Modifiers(codecs=["ZSTD(1)"]))),
    Column("is_segment", Bool()),
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
        "duration_ms",
        UInt(32, modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
    ),
    Column("exclusive_time_ms", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column(
        "retention_days",
        UInt(16, modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
    ),
    Column("name", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sampling_weight", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sign", Int(8, modifiers=Modifiers(codecs=["DoubleDelta(1)"]))),
]
columns.extend(
    [
        Column(
            f"attr_str_{i}",
            Map(String(), String(), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(50)
    ]
)

columns.extend(
    [
        Column(
            f"attr_num_{i}",
            Map(String(), Float(64), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(50)
    ]
)
columns.extend(
    [
        Column(
            f"attr_bool_{i}",
            Map(String(), Bool(), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(10)
    ]
)

index_create_ops: Sequence[SqlOperation] = (
    [
        operations.AddIndex(
            storage_set=storage_set_name,
            table_name=local_table_name,
            index_name="bf_trace_id",
            index_expression="trace_id",
            index_type="bloom_filter",
            granularity=1,
            target=OperationTarget.LOCAL,
        ),
    ]
    + [
        operations.AddIndex(
            storage_set=storage_set_name,
            table_name=local_table_name,
            index_name=f"bf_attr_str_{i}",
            index_expression=f"mapKeys(attr_str_{i})",
            index_type="bloom_filter",
            granularity=1,
            target=OperationTarget.LOCAL,
        )
        for i in range(50)
    ]
    + [
        operations.AddIndex(
            storage_set=storage_set_name,
            table_name=local_table_name,
            index_name=f"bf_attr_str_val_{i}",
            index_expression=f"mapValues(attr_str_{i})",
            index_type="ngrambf_v1(4, 1024, 10, 1)",
            granularity=1,
            target=OperationTarget.LOCAL,
        )
        for i in range(50)
    ]
    + [
        operations.AddIndex(
            storage_set=storage_set_name,
            table_name=local_table_name,
            index_name=f"bf_attr_num_{i}",
            index_expression=f"mapKeys(attr_num_{i})",
            index_type="bloom_filter",
            granularity=1,
            target=OperationTarget.LOCAL,
        )
        for i in range(50)
    ]
    + [
        operations.AddIndex(
            storage_set=storage_set_name,
            table_name=local_table_name,
            index_name=f"bf_attr_bool_{i}",
            index_expression=f"mapKeys(attr_bool_{i})",
            index_type="bloom_filter",
            granularity=1,
            target=OperationTarget.LOCAL,
        )
        for i in range(10)
    ]
)


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        res: List[SqlOperation] = [
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.CollapsingMergeTree(
                    primary_key="(organization_id, _sort_timestamp, trace_id)",
                    order_by="(organization_id, _sort_timestamp, trace_id, span_id)",
                    sign_column="sign",
                    partition_by="(toMonday(_sort_timestamp))",
                    settings={"index_granularity": "8192"},
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
                    sharding_key="reinterpretAsUInt128(trace_id)",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
        res.extend(index_create_ops)
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
