import logging

from datetime import datetime
from enum import Enum

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.migration import App
from snuba.migrations.apps import get_app

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
    def run_migration(self, app: App, migration_id: str) -> None:
        """
        Run a single migration given its ID and marks the migration as complete.
        """
        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        logger.info(f"Running migration: {app} {migration_id}")

        migration = get_app(app).load_migration(migration_id)

        operations = migration.forwards_local()
        for op in operations:
            op.execute()

        logger.info(f"Finished running, updating status: {app} {migration_id}")

        self._mark_completed(app, migration_id)

        logger.info(f"Finished: {app} {migration_id}")

    def _mark_completed(self, app: App, migration_id: str) -> None:
        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "app": app.value,
                "migration_id": migration_id,
                "timestamp": datetime.now(),
                "status": Status.COMPLETED.value,
            }
        ]
        connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        connection.execute(statement, data)
