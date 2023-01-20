from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds indexes and codecs to match schema in SaaS.
    Clickhouse 20 doesn't support adding codecs to key columns, so we don't add codecs to span_id and event_id.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                Column("event_id", UUID(Modifiers(nullable=True, codecs=["LZ4HC(0)"]))),
                ttl_month=("timestamp", 1),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_key_id",
                index_expression="key_id",
                index_type="minmax",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_outcome",
                index_expression="outcome",
                index_type="minmax",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # Clickhouse 20 doesn't support resetting the codec to the default.
            # so the modify is a no-op that can become effective when we upgrade to 21.
            operations.ModifyColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                Column("event_id", UUID(Modifiers(nullable=True))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_key_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_outcome",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
