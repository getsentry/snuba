from typing import List, Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import UUID, Bool, DateTime, Float, Int, Map, String

num_attr_buckets = 40
storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
sampling_weights = [8, 8**2, 8**3]
old_version = 3
new_version = old_version + 1

columns: List[Column[Modifiers]] = [
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


def should_include_column(name: str) -> bool:
    return name not in {
        "sampling_weight",
        "sampling_factor",
        "retention_days",
        "client_sample_rate",
        "server_sample_rate",
    }


def generate_old_materialized_view_expression(sampling_weight: int) -> str:
    column_names_str = ", ".join([c.name for c in columns if should_include_column(c.name)])
    return " ".join(
        [
            "SELECT",
            f"{column_names_str},",
            "downsampled_retention_days AS retention_days,",
            f"sampling_weight * {sampling_weight} AS sampling_weight,",
            f"sampling_factor / {sampling_weight} AS sampling_factor",
            "FROM eap_items_1_local",
            f"WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0",
        ]
    )


def generate_new_materialized_view_expression(sampling_weight: int) -> str:
    column_names_str = ", ".join([c.name for c in columns if should_include_column(c.name)])
    return " ".join(
        [
            "SELECT",
            f"{column_names_str},",
            "downsampled_retention_days AS retention_days,",
            f"sampling_weight * {sampling_weight} AS sampling_weight,",
            f"sampling_factor / {sampling_weight} AS sampling_factor,",
            f"client_sample_rate / {sampling_weight} AS client_sample_rate,",
            f"server_sample_rate / {sampling_weight} AS server_sample_rate",
            "FROM eap_items_1_local",
            f"WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0",
        ]
    )


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        for sampling_weight in sampling_weights:
            local_table_name = f"eap_items_1_downsample_{sampling_weight}_local"

            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=storage_set_key,
                        view_name=f"eap_items_1_downsample_{sampling_weight}_mv_{new_version}",
                        columns=columns,
                        destination_table_name=local_table_name,
                        target=OperationTarget.LOCAL,
                        query=generate_new_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=storage_set_key,
                        table_name=f"eap_items_1_downsample_{sampling_weight}_mv_{old_version}",
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []

        for sampling_weight in sampling_weights:
            local_table_name = f"eap_items_1_downsample_{sampling_weight}_local"

            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=storage_set_key,
                        view_name=f"eap_items_1_downsample_{sampling_weight}_mv_{old_version}",
                        columns=columns,
                        destination_table_name=local_table_name,
                        target=OperationTarget.LOCAL,
                        query=generate_old_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=storage_set_key,
                        table_name=f"eap_items_1_downsample_{sampling_weight}_mv_{new_version}",
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops
