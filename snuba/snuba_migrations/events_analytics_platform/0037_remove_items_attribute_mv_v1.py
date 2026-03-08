from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations.operations import SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    """
    Deprecated: This migration previously removed items attribute MVs.
    These tables are no longer used and have been removed.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return []

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
