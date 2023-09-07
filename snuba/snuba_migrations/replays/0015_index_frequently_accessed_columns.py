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
    for column in materialized_columns:
        yield operations.AddIndex(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            index_name=f"bf_{column}",
            index_expression=column,
            index_type="bloom_filter()",
            granularity=1,
            target=operations.OperationTarget.LOCAL,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column in materialized_columns:
        yield operations.DropIndex(
            StorageSetKey.REPLAYS,
            "replays_local",
            f"bf_{column}",
            target=operations.OperationTarget.LOCAL,
        )


materialized_columns = ["urls", "user_id", "user_name", "user_email", "ip_address_v4"]
