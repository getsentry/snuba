import logging

from datetime import datetime
from enum import Enum

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.groups import get_group_loader, MigrationGroup

logger = logging.getLogger("snuba.migrations")

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"

# Currently only local migrations are supported
TABLE_NAME = LOCAL_TABLE_NAME


class Status(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class Runner:
    def run_migration(self, group: MigrationGroup, migration_id: str) -> None:
        """
        Run a single migration given its ID and marks the migration as complete.
        """
        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        logger.info(f"Running migration: {group} {migration_id}")

        migration = get_group_loader(group).load_migration(migration_id)

        operations = migration.forwards_local()
        for op in operations:
            op.execute()

        logger.info(f"Finished running, updating status: {group} {migration_id}")

        # TODO: In addition to marking migrations as completed, we should also mark
        # migrations as in-progress before we execute the operations. However we
        # will need to have some mechanism that allows this to be skipped in certain
        # cases, such as the initial migration that creates the migrations table itself.
        self._mark_completed(group, migration_id)

        logger.info(f"Finished: {group} {migration_id}")

    def _mark_completed(self, group: MigrationGroup, migration_id: str) -> None:
        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "group": group.value,
                "migration_id": migration_id,
                "timestamp": datetime.now(),
                "status": Status.COMPLETED.value,
                # TODO: Version should be incremented each time we update that
                # migration status
                "version": 1,
            }
        ]
        connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        connection.execute(statement, data)
