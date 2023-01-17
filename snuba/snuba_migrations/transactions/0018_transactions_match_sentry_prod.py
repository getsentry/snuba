from typing import Sequence

from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    The app start type column is required to query for different
    app start types.
    """

    blocking = False

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return []
        # return [operations.AlterTable()]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return []
        # return [operations.ModifyColumn()]
