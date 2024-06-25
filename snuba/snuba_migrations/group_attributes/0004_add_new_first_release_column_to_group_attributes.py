from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return []

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
