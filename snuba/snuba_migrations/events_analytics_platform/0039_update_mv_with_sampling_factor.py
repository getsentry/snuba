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

columns_with_sampling_factor = columns
columns_with_sampling_factor.insert(
    7,
    Column("sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
)


def get_mv_expr(sampling_weight: int) -> str:
    """
    we use sampling_weight for our calculations, which is 1/sampling_weight which means that when we downsample in storage, the downsampled sampling weight should be multiplied by the downsample rate.
    Example:
        original_sampling_weight = 1 // this means the sampling rate of the item was 1.0
        tier_1_sampling_weight = 8 // tier 1 takes every 8 items
        tier_2_sampling_weight = 64 // tier 2 takes every 64 items from tier 0
        tier_3_sampling_weight = 512 // tier 3 takes every 512 items from tier 0
    """
    column_names_str = ", ".join(
        [f"{c.name} AS {c.name}" for c in columns if c.name != "sampling_weight"]
    )
    # NOTE (Volo): We're doing somethign a little sketchy here.
    # We want sampling to be random across tiers (i.e. each tier is a random sample of the source table),
    # `cityHash64(item_id) % sampling_weight = 0` would make it so that each tier was a strict subset of the previous tier
    # we would rather have `rand64() % sampling_weight` as our way to select a sample
    # !!!! BUT !!!!!
    # when testing locally, using `rand64()` to select the sample, made actual sample rates between tiers be off by an order of magnitude (and unpredictably so)
    # in order to achieve the goal while avoiding what seems to be a bug in clickhouse we do the following:

    # (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0
    #             ^^^^^^^^^^^^^^^^^^^^^^^
    # Add the sampling_weight to the item id before hashing it. This assures that every tier will generate a different hash make a sampling decision.
    # item_ids are random already, hashing the sum assures enough randomness for our purposes and makes the sampling deterministic which may be easier
    # to reason about and test
    return f"SELECT {column_names_str}, sampling_weight * {sampling_weight} AS sampling_weight FROM eap_items_1_local WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0"


def get_mv_expr_sampling_factor(sampling_weight: int) -> str:
    """
    same as above but we're also adding the sampling factor to the mv
    """
    column_names_str = ", ".join(
        [
            f"{c.name} AS {c.name}"
            for c in columns
            if c.name != "sampling_weight" and c.name != "sampling_factor"
        ]
    )
    return f"SELECT {column_names_str}, sampling_weight * {sampling_weight} AS sampling_weight, sampling_factor / {sampling_weight} AS sampling_factor FROM eap_items_1_local WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0"


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    sampling_weights = [8, 8**2, 8**3]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sampling_weight in self.sampling_weights:
            local_table_name = f"eap_items_1_downsample_{sampling_weight}_local"
            mv_name = f"eap_items_1_downsample_{sampling_weight}_mv_2"
            mv_query = get_mv_expr_sampling_factor(sampling_weight)
            ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=mv_name,
                    columns=columns_with_sampling_factor,
                    destination_table_name=local_table_name,
                    target=OperationTarget.LOCAL,
                    query=mv_query,
                )
            )

        for sampling_weight in self.sampling_weights:
            mv_name = f"eap_items_1_downsample_{sampling_weight}_mv"

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

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops = []
        for sampling_weight in self.sampling_weights:
            local_table_name = f"eap_items_1_downsample_{sampling_weight}_local"
            mv_name = f"eap_items_1_downsample_{sampling_weight}_mv"
            mv_query = get_mv_expr(sampling_weight)
            ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=mv_name,
                    columns=columns_with_sampling_factor,
                    destination_table_name=local_table_name,
                    target=OperationTarget.LOCAL,
                    query=mv_query,
                )
            )

        for sampling_weight in self.sampling_weights:
            mv_name = f"eap_items_1_downsample_{sampling_weight}_mv_2"

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
