from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the http columns defined, with the method and referer coming from the request interface
    and url materialized from the tags.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column(
                    "http_method",
                    String(Modifiers(nullable=True, low_cardinality=True)),
                ),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("http_referer", String(Modifiers(nullable=True))),
                after="http_method",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.EVENTS, "errors_local", "http_method"),
            operations.DropColumn(StorageSetKey.EVENTS, "errors_local", "http_referer"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column(
                    "http_method",
                    String(Modifiers(nullable=True, low_cardinality=True)),
                ),
                after="sdk_version",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("http_referer", String(Modifiers(nullable=True))),
                after="http_method",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.EVENTS, "errors_dist", "http_method"),
            operations.DropColumn(StorageSetKey.EVENTS, "errors_dist", "http_referer"),
        ]
