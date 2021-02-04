from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

# TODO: materialized view migration


class Migration(migration.MultiStepMigration):
    """
    Adds the http columns defined, with the method and referer coming from the request interface
    and url materialized from the tags.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("size", UInt(32, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("category", UInt(8)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("size", UInt(32)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("category", UInt(8)),
                after=None,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.OUTCOMES, "outcomes_raw_local", "size"),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_local", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "size"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "category"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column=Column("size", UInt(32, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column=Column("category", UInt(8)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_dist",
                column=Column("size", UInt(32)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_dist",
                column=Column("category", UInt(8)),
                after=None,
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.OUTCOMES, "outcomes_raw_dist", "size"),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_dist", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_dist", "size"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_dist", "category"
            ),
        ]
