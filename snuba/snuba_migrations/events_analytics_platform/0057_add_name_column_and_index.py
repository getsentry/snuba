from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO
table_name_prefix = "eap_items_1"
new_columns = [
    Column(
        "indexed_name",
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

        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        local_table_names = [local_table_name] + [
            f"eap_items_1_downsample_{sampling_weight}_local"
            for sampling_weight in sampling_weights
        ]
        ops: list[operations.SqlOperation] = [
            operations.DropIndex(
                storage_set=storage_set,
                table_name=table_name,
                index_name=index_name,
                target=OperationTarget.LOCAL,
            )
            for table_name in local_table_names
        ]

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
