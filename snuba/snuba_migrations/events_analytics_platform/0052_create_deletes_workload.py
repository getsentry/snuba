from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        create_all_workload = "CREATE WORKLOAD IF NOT EXISTS all"

        create_deletes_workload = """
            CREATE WORKLOAD IF NOT EXISTS low_priority_deletes
            IN all
            SETTINGS
                priority = 100,
                max_requests = 2;
        """

        return [
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=create_all_workload,
                target=OperationTarget.LOCAL,
            ),
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=create_deletes_workload,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        drop_deletes_workload = "DROP WORKLOAD IF EXISTS low_priority_deletes;"
        drop_all_workload = "DROP WORKLOAD IF EXISTS all;"

        return [
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=drop_deletes_workload,
                target=OperationTarget.LOCAL,
            ),
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=drop_all_workload,
                target=OperationTarget.LOCAL,
            ),
        ]
