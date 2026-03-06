from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.migration_utilities import get_clickhouse_version_for_storage_set
from snuba.migrations.operations import OperationTarget


class _AlterWorkloadOp(operations.RunSql):
    """Version-aware RunSql that picks the correct thread setting at execution time."""

    def __init__(self) -> None:
        # Placeholder statement for validation; actual SQL is generated in execute()
        super().__init__(
            storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
            statement="SELECT 1",
            target=OperationTarget.LOCAL,
        )

    def _build_statement(self, thread_setting: str) -> str:
        return f"""
            CREATE OR REPLACE WORKLOAD low_priority_deletes
            IN all
            SETTINGS
                priority = 100,
                max_requests = 2,
                {thread_setting} = 4;
        """

    def execute(self) -> None:
        ch_version = get_clickhouse_version_for_storage_set(self._storage_set, None)

        # max_threads was renamed to max_concurrent_threads in ClickHouse 25.8+
        if ch_version >= (25, 8):
            thread_setting = "max_concurrent_threads"
        else:
            thread_setting = "max_threads"

        resolved = operations.RunSql(
            storage_set=self._storage_set,
            statement=self._build_statement(thread_setting),
            target=OperationTarget.LOCAL,
        )
        resolved.execute()


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [_AlterWorkloadOp()]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # Restore original settings without thread limit
        alter_workload = """
            CREATE OR REPLACE WORKLOAD low_priority_deletes
            IN all
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
