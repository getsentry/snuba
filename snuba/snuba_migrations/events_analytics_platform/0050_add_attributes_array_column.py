from snuba.clickhouse.columns import JSON, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name_prefix = "eap_items_1"
new_columns = [
    Column(
        "attributes_array",
        JSON(
            max_dynamic_paths=4096,
            max_dynamic_types=32,
            modifiers=Modifiers(
                codecs=[
                    "ZSTD(1)",
                ],
            ),
        ),
    ),
]
after = "attributes_float_39"
sampling_weights = [8, 8**2, 8**3]


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

        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = [
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

        return ops
