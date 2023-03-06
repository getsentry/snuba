from typing import Iterator, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.SqlOperation]:
    yield operations.AddIndex(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_local",
        index_name="bf_replay_id",
        index_expression="replay_id",
        index_type="bloom_filter()",
        granularity=1,
        target=operations.OperationTarget.LOCAL,
    )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    yield operations.DropIndex(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_local",
        index_name="bf_replay_id",
        target=operations.OperationTarget.LOCAL,
    )
