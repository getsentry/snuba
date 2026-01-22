from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, UInt
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    # Kafka topic offset
    Column("offset", UInt(64)),
    # GroupStatus in Sentry does not have a 'DELETED' state that reflects the deletion
    # of the record. Having a dedicated clickhouse-only flag to identify this case seems
    # more consistent than add an additional value into the status field below that does not
    # exists on the Sentry side.
    Column("record_deleted", UInt(8)),
    # PG columns
    Column("project_id", UInt(64)),
    Column("id", UInt(64)),
    Column("status", UInt(8, Modifiers(nullable=True))),
    Column("last_seen", DateTime(Modifiers(nullable=True))),
    Column("first_seen", DateTime(Modifiers(nullable=True))),
    Column("active_at", DateTime(Modifiers(nullable=True))),
    Column("first_release_id", UInt(64, Modifiers(nullable=True))),
]


# NOTE: CDC storage deprecated
class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
