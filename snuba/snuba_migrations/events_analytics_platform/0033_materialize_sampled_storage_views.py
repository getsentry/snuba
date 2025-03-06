from typing import List, Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
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
            ),
        )
        for i in range(num_attr_buckets)
    ]
)


def get_mv_expr(sampling_rate: int) -> str:
    """
    we use sampling_weight for our calculations, which is 1/sample_rate which means that when we downsample in storage, the downsampled sampling weight should be multiplied by the downsample rate.
    Example:
        original_sampling_weight = 1 // this means the sampling rate of the item was 1.0
        tier_1_sampling_rate = 8 // tier 1 takes every 8 items
        tier_2_sampling_rate = 64 // tier 2 takes every 8 items from tier 1
        tier_3_sampling_rate = 512 // tier 3 takes every 8 items from tier 2
    """
    column_names_str = ", ".join(
        [f"{c.name} AS {c.name}" for c in columns if c.name != "sampling_weight"]
    )

    # set sampling weight explicitly in mv
    return f"SELECT {column_names_str}, sampling_weight * {sampling_rate} AS sampling_weight FROM eap_items_1_local WHERE (cityHash64(item_id) % {sampling_rate}) = 0"


storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    sampling_rates = [8, 8**2, 8**3]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sample_rate in self.sampling_rates:
            local_table_name = f"eap_items_1_downsample_{sample_rate}_local"
            mv_name = f"eap_items_1_downsample_{sample_rate}_mv"
            mv_query = get_mv_expr(sample_rate)
            ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=mv_name,
                    columns=columns,
                    destination_table_name=local_table_name,
                    target=OperationTarget.LOCAL,
                    query=mv_query,
                )
            )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sample_rate in self.sampling_rates:
            mv_name = f"eap_items_1_downsample_{sample_rate}_mv"

            ops.extend(
                [
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=mv_name,
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )
        return ops
