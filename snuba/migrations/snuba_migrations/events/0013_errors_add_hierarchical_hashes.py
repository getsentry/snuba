from typing import Sequence

from snuba.clickhouse.columns import Column, FixedString, Array, UUID
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


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
                column=Column("hierarchical_hashes", Array(UUID()),),
                after="primary_hash",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column("hierarchical_hashes", Array(FixedString(32)),),
                after="primary_hash",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS, "errors_local", "hierarchical_hashes"
            ),
            operations.DropColumn(
                StorageSetKey.EVENTS, "sentry_local", "hierarchical_hashes"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("hierarchical_hashes", Array(UUID())),
                after="primary_hash",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_dist",
                column=Column("hierarchical_hashes", Array(FixedString(32)),),
                after="primary_hash",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS, "errors_dist", "hierarchical_hashes"
            ),
            operations.DropColumn(
                StorageSetKey.EVENTS, "sentry_dist", "hierarchical_hashes"
            ),
        ]
