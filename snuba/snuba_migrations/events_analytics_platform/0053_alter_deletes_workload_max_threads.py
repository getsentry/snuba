from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        alter_workload = """
            ALTER WORKLOAD low_priority_deletes
            SETTINGS
                priority = 100,
                max_requests = 2,
                max_threads = 4;
        """

        return [
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=alter_workload,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # Restore original settings without max_threads
        alter_workload = """
            ALTER WORKLOAD low_priority_deletes
            SETTINGS
                priority = 100,
                max_requests = 2;
        """

        return [
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=alter_workload,
                target=OperationTarget.LOCAL,
            ),
        ]
