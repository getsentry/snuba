from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import UUID, Bool, DateTime, Float, Int, Map, UInt

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO
table_name_prefix = "eap_items_1"
column_name = "indexed_name"
new_columns = [
    Column(
        column_name,
        String(
            Modifiers(
                codecs=[
                    "ZSTD(1)",
                ],
            ),
        ),
    ),
]
after = "retention_days"
sampling_weights = [8, 8**2, 8**3]
local_table_name = f"{table_name_prefix}_local"
index_name = "bf_indexed_name"
index_expression = "indexed_name"
index_type = "bloom_filter"
index_granularity = 1

# Materialized views that feed the downsampled tables select an explicit column
# list from eap_items_1_local, so the new column has to be added to the views
# (not just the tables) for it to be populated on the downsampled read paths.
# These columns mirror the current mv_4 definition (migration 0049) with the new
# `indexed_name` column inserted after `retention_days`.
num_attr_buckets = 40
mv_old_version = 4
mv_new_version = mv_old_version + 1

mv_columns: list[Column[Modifiers]] = [
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
    new_columns[0],
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
mv_columns.extend(
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
mv_columns.extend(
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


def _should_passthrough_column(name: str) -> bool:
    # Columns computed in the SELECT rather than copied straight through.
    return name not in {
        "sampling_weight",
        "sampling_factor",
        "retention_days",
        "client_sample_rate",
        "server_sample_rate",
    }


def _materialized_view_query(sampling_weight: int, include_indexed_name: bool) -> str:
    passthrough = [
        c.name
        for c in mv_columns
        if _should_passthrough_column(c.name) and (include_indexed_name or c.name != column_name)
    ]
    return " ".join(
        [
            "SELECT",
            f"{', '.join(passthrough)},",
            "downsampled_retention_days AS retention_days,",
            f"sampling_weight * {sampling_weight} AS sampling_weight,",
            f"sampling_factor / {sampling_weight} AS sampling_factor,",
            f"client_sample_rate / {sampling_weight} AS client_sample_rate,",
            f"server_sample_rate / {sampling_weight} AS server_sample_rate",
            "FROM eap_items_1_local",
            f"WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0",
        ]
    )


def _recreate_materialized_views_ops(
    create_version: int,
    drop_version: int,
    columns: list[Column[Modifiers]],
    include_indexed_name: bool,
) -> list[operations.SqlOperation]:
    ops: list[operations.SqlOperation] = []
    for sampling_weight in sampling_weights:
        downsampled_local_table = f"eap_items_1_downsample_{sampling_weight}_local"
        ops.extend(
            [
                operations.CreateMaterializedView(
                    storage_set=storage_set,
                    view_name=f"eap_items_1_downsample_{sampling_weight}_mv_{create_version}",
                    columns=columns,
                    destination_table_name=downsampled_local_table,
                    target=OperationTarget.LOCAL,
                    query=_materialized_view_query(sampling_weight, include_indexed_name),
                ),
                operations.DropTable(
                    storage_set=storage_set,
                    table_name=f"eap_items_1_downsample_{sampling_weight}_mv_{drop_version}",
                    target=OperationTarget.LOCAL,
                ),
            ]
        )
    return ops


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_{suffix}",
                column=new_column,
                after=after,
                target=target,
            )
            for suffix, target in [
                ("local", OperationTarget.LOCAL),
                ("dist", OperationTarget.DISTRIBUTED),
            ]
            for new_column in new_columns
        ]

        for sampling_weight in sampling_weights:
            downsampled_table_prefix = f"eap_items_1_downsample_{sampling_weight}"

            ops.extend(
                [
                    operations.AddColumn(
                        storage_set=storage_set,
                        table_name=f"{downsampled_table_prefix}_{suffix}",
                        column=new_column,
                        after=after,
                        target=target,
                    )
                    for suffix, target in [
                        ("local", OperationTarget.LOCAL),
                        ("dist", OperationTarget.DISTRIBUTED),
                    ]
                    for new_column in new_columns
                ]
            )

        # The read-only distributed tables (created via `CREATE TABLE ... AS`)
        # do not inherit schema changes from their source tables, so the column
        # must be added explicitly for queries on the read path to see it.
        ro_table_names = [f"{table_name_prefix}_dist_ro"] + [
            f"eap_items_1_downsample_{sampling_weight}_dist_ro"
            for sampling_weight in sampling_weights
        ]
        ops.extend(
            [
                operations.AddColumn(
                    storage_set=ro_storage_set,
                    table_name=ro_table_name,
                    column=new_column,
                    after=after,
                    target=OperationTarget.DISTRIBUTED,
                )
                for ro_table_name in ro_table_names
                for new_column in new_columns
            ]
        )

        local_table_names = [local_table_name] + [
            f"eap_items_1_downsample_{sampling_weight}_local"
            for sampling_weight in sampling_weights
        ]
        ops.extend(
            [
                operations.AddIndex(
                    storage_set=storage_set,
                    table_name=table_name,
                    index_name=index_name,
                    index_expression=index_expression,
                    index_type=index_type,
                    granularity=index_granularity,
                    target=OperationTarget.LOCAL,
                )
                for table_name in local_table_names
            ]
        )

        # Recreate the downsample materialized views so they select the new
        # column and populate it on the downsampled tables. The column must
        # already exist on both eap_items_1_local and the downsampled tables
        # (added above) before the views are recreated.
        ops.extend(
            _recreate_materialized_views_ops(
                create_version=mv_new_version,
                drop_version=mv_old_version,
                columns=mv_columns,
                include_indexed_name=True,
            )
        )

        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        # Restore the previous materialized views (which do not reference the
        # new column) before dropping the column the new views read from.
        ops: list[operations.SqlOperation] = _recreate_materialized_views_ops(
            create_version=mv_old_version,
            drop_version=mv_new_version,
            columns=[c for c in mv_columns if c.name != column_name],
            include_indexed_name=False,
        )

        local_table_names = [local_table_name] + [
            f"eap_items_1_downsample_{sampling_weight}_local"
            for sampling_weight in sampling_weights
        ]
        ops.extend(
            [
                operations.DropIndex(
                    storage_set=storage_set,
                    table_name=table_name,
                    index_name=index_name,
                    target=OperationTarget.LOCAL,
                )
                for table_name in local_table_names
            ]
        )

        ops.extend(
            [
                operations.DropColumn(
                    storage_set=storage_set,
                    table_name=f"{table_name_prefix}_{suffix}",
                    column_name=new_column.name,
                    target=target,
                )
                for suffix, target in [
                    ("dist", OperationTarget.DISTRIBUTED),
                    ("local", OperationTarget.LOCAL),
                ]
                for new_column in new_columns
            ]
        )

        for sampling_weight in sampling_weights:
            downsampled_table_prefix = f"eap_items_1_downsample_{sampling_weight}"

            ops.extend(
                [
                    operations.DropColumn(
                        storage_set=storage_set,
                        table_name=f"{downsampled_table_prefix}_{suffix}",
                        column_name=new_column.name,
                        target=target,
                    )
                    for suffix, target in [
                        ("dist", OperationTarget.DISTRIBUTED),
                        ("local", OperationTarget.LOCAL),
                    ]
                    for new_column in new_columns
                ]
            )

        ro_table_names = [f"{table_name_prefix}_dist_ro"] + [
            f"eap_items_1_downsample_{sampling_weight}_dist_ro"
            for sampling_weight in sampling_weights
        ]
        ops.extend(
            [
                operations.DropColumn(
                    storage_set=ro_storage_set,
                    table_name=ro_table_name,
                    column_name=new_column.name,
                    target=OperationTarget.DISTRIBUTED,
                )
                for ro_table_name in ro_table_names
                for new_column in new_columns
            ]
        )

        return ops
