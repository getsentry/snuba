from typing import Sequence

from snuba.migrations import migration
from snuba.migrations.operations import SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return []

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
