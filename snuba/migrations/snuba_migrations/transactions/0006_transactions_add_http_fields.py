from typing import Sequence

from snuba.clickhouse.columns import (
    Column,
    Nullable,
    String,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import LowCardinality


class Migration(migration.MultiStepMigration):
    """
    Adds the http columns populated from the Request interface that is missing from transactions.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("http_method", LowCardinality(Nullable(String()))),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("http_referer", Nullable(String())),
                after="http_method",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "http_method"
            ),
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "http_referer"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column("http_method", LowCardinality(Nullable(String()))),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column("http_referer", Nullable(String())),
                after="http_method",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "http_method"
            ),
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "http_referer"
            ),
        ]
