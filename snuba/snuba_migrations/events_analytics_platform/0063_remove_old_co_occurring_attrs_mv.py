from collections.abc import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    """
    Stop writing to the v1 co-occurring attributes table.

    Reads have moved (or are moving) to ``eap_item_co_occurring_attrs_2_*``
    via ``eap_item_co_occurring_attrs_3_mv``. Dropping this view ends the
    dual-write; the v1 local/dist tables are left in place for a later drop.

    This is not reversed: recreating ``eap_item_co_occurring_attrs_2_mv``
    would resume dual-writes into v1, which we do not want.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    mv_name = "eap_item_co_occurring_attrs_2_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.mv_name,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
