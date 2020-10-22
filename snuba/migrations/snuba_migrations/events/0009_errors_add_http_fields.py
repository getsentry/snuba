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
    Adds the http columns defined, with the method and referer coming from the request interface
    and url materialized from the tags.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("http_method", String([Nullable(), LowCardinality()])),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("http_referer", String([Nullable()])),
                after="http_method",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.EVENTS, "errors_local", "http_method"),
            operations.DropColumn(StorageSetKey.EVENTS, "errors_local", "http_referer"),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("http_method", String([Nullable(), LowCardinality()])),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("http_referer", String([Nullable()])),
                after="http_method",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.EVENTS, "errors_dist", "http_method"),
            operations.DropColumn(StorageSetKey.EVENTS, "errors_dist", "http_referer"),
        ]
