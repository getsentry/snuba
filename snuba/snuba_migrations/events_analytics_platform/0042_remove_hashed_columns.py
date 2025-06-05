from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import (
    UUID,
    Array,
    Bool,
    Column,
    DateTime,
    Float,
    Int,
    Map,
    String,
    UInt,
)

buckets = 40


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


base_columns = [
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

base_columns.extend(
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
        for i in range(buckets)
    ]
)

base_columns.extend(
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
        for i in range(buckets)
    ]
)

hash_map_columns = [
    Column(
        hash_map_column_name("string", i),
        Array(
            UInt(64),
        ),
    )
    for i in range(buckets)
]

sampling_factor_column = Column(
    "sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))
)


def get_mv_expr(sampling_weight: int, with_hashed_columns: bool = True) -> str:
    column_names = [
        c.name
        for c in base_columns
        if c.name not in {"sampling_weight", "sampling_factor"}
    ]
    if with_hashed_columns:
        column_names.extend([f"{c.name}" for c in (hash_map_columns)])
    column_names_str = ", ".join(column_names)
    return f"SELECT {column_names_str}, sampling_weight * {sampling_weight} AS sampling_weight, sampling_factor / {sampling_weight} AS sampling_factor FROM eap_items_1_local WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"
    downsampled_factors = [8, 64, 512]

    local_table_name = "eap_items_1_local"
    dist_table_name = "eap_items_1_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        ops.append(
            operations.DropIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_hashed_keys",
                target=OperationTarget.LOCAL,
            ),
        )
        for downsampled_factor in self.downsampled_factors:
            ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=f"eap_items_1_downsample_{downsampled_factor}_mv_3",
                    columns=base_columns + [sampling_factor_column],
                    destination_table_name=f"eap_items_1_downsample_{downsampled_factor}_local",
                    target=OperationTarget.LOCAL,
                    query=get_mv_expr(downsampled_factor, with_hashed_columns=False),
                )
            )
            ops.append(
                operations.DropTable(
                    storage_set=self.storage_set_key,
                    table_name=f"eap_items_1_downsample_{downsampled_factor}_mv_2",
                    target=OperationTarget.LOCAL,
                )
            )
        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        for downsampled_factor in self.downsampled_factors:
            ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=f"eap_items_1_downsample_{downsampled_factor}_mv_2",
                    columns=base_columns + [sampling_factor_column],
                    destination_table_name=f"eap_items_1_downsample_{downsampled_factor}_local",
                    target=OperationTarget.LOCAL,
                    query=get_mv_expr(downsampled_factor, with_hashed_columns=True),
                )
            )
            ops.append(
                operations.DropTable(
                    storage_set=self.storage_set_key,
                    table_name=f"eap_items_1_downsample_{downsampled_factor}_mv_3",
                    target=OperationTarget.LOCAL,
                )
            )
        ops.append(
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_hashed_keys",
                index_expression="hashed_keys",
                index_type="bloom_filter",
                granularity=1,
                target=OperationTarget.LOCAL,
            ),
        )
        return ops
