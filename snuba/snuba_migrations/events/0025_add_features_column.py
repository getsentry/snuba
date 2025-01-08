from typing import Iterator

from snuba.clickhouse.columns import Array, Column, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import FEATURES_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

table_names = [
    ("errors_local", StorageSetKey.EVENTS),
    ("errors_dist", StorageSetKey.EVENTS),
    ("errors_dist_ro", StorageSetKey.EVENTS_RO),
]
targets = [OperationTarget.LOCAL, OperationTarget.DISTRIBUTED]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Iterator[operations.SqlOperation]:
        # Add local and dist columns for the three tables.
        for target in targets:
            for table_name, storage_set in table_names:
                yield from [
                    operations.AddColumn(
                        storage_set=storage_set,
                        table_name=table_name,
                        column=Column(
                            "features", Nested([("key", String()), ("value", String())])
                        ),
                        after="_tags_hash_map",
                        target=target,
                    ),
                    operations.AddColumn(
                        storage_set=storage_set,
                        table_name=table_name,
                        column=Column(
                            "_features_hash_map",
                            Array(
                                UInt(64),
                                Modifiers(materialized=FEATURES_HASH_MAP_COLUMN),
                            ),
                        ),
                        after="features.value",
                        target=target,
                    ),
                ]

        # Add index to the local table.
        yield operations.AddIndex(
            storage_set=StorageSetKey.EVENTS,
            table_name="errors_local",
            index_name="bf_features_hash_map",
            index_expression="_features_hash_map",
            index_type="bloom_filter",
            granularity=1,
            target=OperationTarget.LOCAL,
        )

    def backwards_ops(self) -> Iterator[operations.SqlOperation]:
        yield operations.DropIndex(
            StorageSetKey.EVENTS,
            "errors_local",
            "bf_features_hash_map",
            target=OperationTarget.LOCAL,
        )

        for target in targets:
            for table_name, storage_set in table_names:
                yield from [
                    operations.DropColumn(
                        storage_set=storage_set,
                        table_name=table_name,
                        column_name="_features_hash_map",
                        target=target,
                    ),
                    operations.DropColumn(
                        storage_set=storage_set,
                        table_name=table_name,
                        column_name="features",
                        target=target,
                    ),
                ]
