from typing import Sequence

from snuba.clickhouse.columns import Array, Column, DateTime, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    This is a one-off migration to support on premise users who are upgrading from
    any older version of Snuba that used the old migration system. Since their sentry_local
    table might be previously created with slightly different columns, this migration
    should bring them back in sync by adding and removing the relevant columns that
    have changed over time. It should be a no-op if the table is already up to date.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("group_id", UInt(64)),
                after="project_id",
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column_name="device_model",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("sdk_integrations", Array(String())),
                after="exception_frames",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("modules.name", Nested([("name", String())])),
                after="sdk_integrations",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("culprit", String(Modifiers(nullable=True))),
                after="sdk_integrations",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("search_message", String(Modifiers(nullable=True))),
                after="received",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("title", String(Modifiers(nullable=True))),
                after="search_message",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("location", String(Modifiers(nullable=True))),
                after="title",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("_tags_flattened", String()),
                after="tags",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("message_timestamp", DateTime()),
                after="partition",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
