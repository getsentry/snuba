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


def attributes_column_name(attribute_type: str, i: int) -> str:
    return f"attributes_{attribute_type}_{i}"


COLUMNS: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("trace_id", UUID()),
    Column("item_id", UInt(128)),
    Column("sampling_weight", UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
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

COLUMNS.extend(
    [
        Column(
            attributes_column_name("string", i),
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
COLUMNS.extend(
    [
        Column(
            hash_map_column_name("string", i),
            Array(
                UInt(64),
            ),
        )
        for i in range(num_attr_buckets)
    ]
    + [
        Column(
            attributes_column_name("float", i),
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

COLUMNS.extend(
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


def local_table_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_local"


def old_materialized_view_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_mv_2"


def old_materialized_view_expression(sampling_weight: int) -> str:
    column_names_str = ", ".join(
        [
            f"{c}"
            for c in COLUMNS
            if c.name != "sampling_weight" and c.name != "sampling_factor"
        ]
    )
    return (
        f"SELECT {column_names_str}, "
        f"sampling_weight * {sampling_weight} AS sampling_weight, "
        f"sampling_factor / {sampling_weight} AS sampling_factor "
        "FROM eap_items_1_local "
        f"WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0"
    )


def new_materialized_view_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_mv_3"


def new_materialized_view_expression(sampling_weight: int) -> str:
    column_names_str = ", ".join(
        [f"{c}" for c in COLUMNS if c != "sampling_weight" and c != "sampling_factor"]
    )
    return (
        f"SELECT {column_names_str}, "
        f"sampling_weight * {sampling_weight} AS sampling_weight, "
        f"sampling_factor / {sampling_weight} AS sampling_factor "
        "FROM eap_items_1_local "
        f"WHERE (cityHash64(trace_id)  % {sampling_weight}) = 0"
    )


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "8192"

    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    sampling_weights = [8, 8**2, 8**3]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        for sampling_weight in self.sampling_weights:
            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=self.storage_set_key,
                        view_name=new_materialized_view_name(sampling_weight),
                        columns=COLUMNS,
                        destination_table_name=local_table_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                        query=new_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=old_materialized_view_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        for sampling_weight in self.sampling_weights:
            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=self.storage_set_key,
                        view_name=old_materialized_view_name(sampling_weight),
                        columns=COLUMNS,
                        destination_table_name=local_table_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                        query=old_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=new_materialized_view_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops
