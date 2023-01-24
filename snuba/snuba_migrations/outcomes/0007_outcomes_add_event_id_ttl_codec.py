from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds TTL and codec to event_id column to match schema in SaaS
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                Column(
                    "event_id",
                    UUID(
                        Modifiers(
                            nullable=True,
                            codecs=["LZ4HC(0)"],
                            ttl="timestamp + toIntervalDay(30)",
                        )
                    ),
                ),
                target=operations.OperationTarget.LOCAL,
            )
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
            )
        ]
