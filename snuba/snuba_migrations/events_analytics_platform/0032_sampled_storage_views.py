from typing import List, Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import AddIndicesData, OperationTarget, SqlOperation
from snuba.utils.schemas import UUID, Bool, DateTime, Float, Int, Map, String

num_attr_buckets = 40


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


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
        "attributes_int",
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
            hash_map_column_name("string", i),
            Array(
                UInt(64),
                Modifiers(
                    materialized=f"arrayMap(k -> cityHash64(k), mapKeys(attributes_string_{i}))",
                ),
            ),
        )
        for i in range(num_attr_buckets)
    ]
)

columns.extend(
    [
        Column(
            f"attributes_float_{i}",
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


columns.extend(
    [
        Column(
            hash_map_column_name("float", i),
            Array(
                UInt(64),
                Modifiers(
                    materialized=f"arrayMap(k -> cityHash64(k), mapKeys(attributes_float_{i}))",
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


MV_QUERY_TEMPLATE = """
    SELECT * FROM eap_items_1_local WHERE cityHash64(trace_id) % {sample_rate} == 0
"""

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    sampling_tiers = {"1": 8, "2": 8**2, "3": 8**3}

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sampling_tier, sample_rate in self.sampling_tiers.items():
            local_table_name = f"eap_items_1_tier_{sampling_tier}_local"
            dist_table_name = f"eap_items_1_tier_{sampling_tier}_dist"
            mv_name = f"eap_items_1_tier_{sampling_tier}_mv"

            ops.extend(
                [
                    operations.CreateTable(
                        storage_set=storage_set_name,
                        table_name=local_table_name,
                        columns=columns,
                        engine=table_engines.ReplacingMergeTree(
                            primary_key="(organization_id, project_id, item_type, timestamp)",
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
                    operations.CreateMaterializedView(
                        storage_set=self.storage_set_key,
                        view_name=mv_name,
                        columns=columns,
                        destination_table_name=local_table_name,
                        target=OperationTarget.LOCAL,
                        query=MV_QUERY_TEMPLATE.format(sample_rate=sample_rate),
                    ),
                ]
            )
        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sampling_tier, _ in self.sampling_tiers.items():
            local_table_name = f"eap_items_1_tier_{sampling_tier}_local"
            dist_table_name = f"eap_items_1_tier_{sampling_tier}_dist"
            mv_name = f"eap_items_1_tier_{sampling_tier}_mv"

            ops.extend(
                [
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=mv_name,
                        target=OperationTarget.LOCAL,
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=local_table_name,
                        target=OperationTarget.LOCAL,
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=dist_table_name,
                        target=OperationTarget.DISTRIBUTED,
                    ),
                ]
            )
        return ops
