from typing import Iterator, Sequence

from snuba.clickhouse.columns import Array, Column, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import FLAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

migration_meta = [
    ("errors_local", StorageSetKey.EVENTS, OperationTarget.LOCAL),
    ("errors_dist", StorageSetKey.EVENTS, OperationTarget.DISTRIBUTED),
    ("errors_dist_ro", StorageSetKey.EVENTS_RO, OperationTarget.DISTRIBUTED),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_ops())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_ops())


def forward_ops() -> Iterator[operations.SqlOperation]:
    # Add local and dist columns for the three tables.
    for table_name, storage_set, target in migration_meta:
        yield from [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=table_name,
                column=Column(
                    "flags", Nested([("key", String()), ("value", String())])
                ),
                after="_tags_hash_map",
                target=target,
            ),
            operations.AddColumn(
                storage_set=storage_set,
                table_name=table_name,
                column=Column(
                    "_flags_hash_map",
                    Array(
                        UInt(64),
                        Modifiers(materialized=FLAGS_HASH_MAP_COLUMN),
                    ),
                ),
                after="flags.value",
                target=target,
            ),
        ]

    # Add index to the local table.
    yield operations.AddIndex(
        storage_set=StorageSetKey.EVENTS,
        table_name="errors_local",
        index_name="bf_flags_hash_map",
        index_expression="_flags_hash_map",
        index_type="bloom_filter",
        granularity=1,
        target=OperationTarget.LOCAL,
    )


def backward_ops() -> Iterator[operations.SqlOperation]:
    yield operations.DropIndex(
        StorageSetKey.EVENTS,
        "errors_local",
        "bf_flags_hash_map",
        target=OperationTarget.LOCAL,
    )

    for table_name, storage_set, target in reversed(migration_meta):
        yield from [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=table_name,
                column_name="_flags_hash_map",
                target=target,
            ),
            operations.DropColumn(
                storage_set=storage_set,
                table_name=table_name,
                column_name="flags",
                target=target,
            ),
        ]
