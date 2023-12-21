from typing import Generator, Sequence, Tuple

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("partition", UInt(16)), "status"),
    (Column("offset", UInt(64)), "partition"),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    Add partition and offset columns to querylog, so a row in clickhouse can be
    correlated with a kafka message
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(_forward())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(_backward())


def _forward() -> Generator[operations.SqlOperation, None, None]:
    for column, after in columns:
        yield operations.AddColumn(
            StorageSetKey.QUERYLOG,
            table_name="querylog_local",
            column=column,
            after=after,
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.AddColumn(
            StorageSetKey.QUERYLOG,
            table_name="querylog_dist",
            column=column,
            after=after,
            target=operations.OperationTarget.DISTRIBUTED,
        )


def _backward() -> Generator[operations.SqlOperation, None, None]:
    for column, after in columns:
        yield operations.DropColumn(
            storage_set=StorageSetKey.QUERYLOG,
            table_name="querylog_dist",
            target=operations.OperationTarget.DISTRIBUTED,
            column_name=column.name,
        )

        yield operations.DropColumn(
            storage_set=StorageSetKey.QUERYLOG,
            table_name="querylog_local",
            target=operations.OperationTarget.LOCAL,
            column_name=column.name,
        )
