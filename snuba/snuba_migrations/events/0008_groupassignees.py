from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, UInt
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    # Kafka topic offset
    Column("offset", UInt(64)),
    Column("record_deleted", UInt(8)),
    # PG columns
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("date_added", DateTime(Modifiers(nullable=True))),
    Column("user_id", UInt(64, Modifiers(nullable=True))),
    Column("team_id", UInt(64, Modifiers(nullable=True))),
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
