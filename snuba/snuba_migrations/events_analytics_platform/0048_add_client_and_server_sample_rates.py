from snuba.clickhouse.columns import Column, Float
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name_prefix = "eap_items_1"
new_column_names = [
    "client_sample_rate",
    "server_sample_rate",
]
after = "sampling_factor"
sampling_weights = [8, 8**2, 8**3]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_{suffix}",
                column=Column(
                    new_column_name,
                    Float(
                        64,
                        modifiers=Modifiers(
                            codecs=[
                                "ZSTD(1)",
                            ]
                        ),
                    ),
                ),
                after=after,
                target=target,
            )
            for suffix, target in [
                ("local", OperationTarget.LOCAL),
                ("dist", OperationTarget.DISTRIBUTED),
            ]
            for new_column_name in new_column_names
        ]

        for sampling_weight in sampling_weights:
            downsampled_table_prefix = f"eap_items_1_downsample_{sampling_weight}"

            ops.extend(
                [
                    operations.AddColumn(
                        storage_set=storage_set,
                        table_name=f"{downsampled_table_prefix}_{suffix}",
                        column=Column(
                            new_column_name,
                            Float(
                                64,
                                modifiers=Modifiers(
                                    codecs=[
                                        "ZSTD(1)",
                                    ]
                                ),
                            ),
                        ),
                        after=after,
                        target=target,
                    )
                    for suffix, target in [
                        ("local", OperationTarget.LOCAL),
                        ("dist", OperationTarget.DISTRIBUTED),
                    ]
                    for new_column_name in new_column_names
                ]
            )

        return ops

    def backwards_ops(self) -> list[operations.SqlOperation]:
        ops: list[operations.SqlOperation] = [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_{suffix}",
                column_name=new_column_name,
                target=target,
            )
            for suffix, target in [
                ("dist", OperationTarget.DISTRIBUTED),
                ("local", OperationTarget.LOCAL),
            ]
            for new_column_name in new_column_names
        ]

        for sampling_weight in sampling_weights:
            downsampled_table_prefix = f"eap_items_1_downsample_{sampling_weight}"

            ops.extend(
                [
                    operations.DropColumn(
                        storage_set=storage_set,
                        table_name=f"{downsampled_table_prefix}_{suffix}",
                        column_name=new_column_name,
                        target=target,
                    )
                    for suffix, target in [
                        ("local", OperationTarget.LOCAL),
                        ("dist", OperationTarget.DISTRIBUTED),
                    ]
                    for new_column_name in new_column_names
                ]
            )

        return ops
