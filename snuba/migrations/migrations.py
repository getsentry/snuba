import logging

from datetime import datetime
from enum import Enum
from importlib import import_module
from typing import NamedTuple, Optional, Sequence

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.operations import Operation


logger = logging.getLogger("snuba.migrations")

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"

# Currently only local migrations are supported
TABLE_NAME = LOCAL_TABLE_NAME


class App(Enum):
    SYSTEM = "system"
    SNUBA = "snuba"


class Status(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class Dependency(NamedTuple):
    app: App
    migration_id: str


class Migration:
    is_dangerous: bool
    dependencies: Optional[Sequence[Dependency]]

    def forwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    def backwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    def forwards_dist(self) -> Sequence[Operation]:
        raise NotImplementedError

    def backwards_dist(self) -> Sequence[Operation]:
        raise NotImplementedError


class Runner:
    def run_migration(self, app: App, migration_id: str) -> None:
        """
        Run a single migration given its ID.
        Updates the migration in the table
        """
        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        logger.info(f"Running migration: {app} {migration_id}")

        migration = self._load_migration(app, migration_id)
        operations = migration.forwards_local()
        for op in operations:
            op.execute()

        logger.info(f"Finished running, updating status: {app} {migration_id}")

        self._mark_completed(app, migration_id)

        logger.info(f"Finished: {app} {migration_id}")

    def _load_migration(self, app: App, migration_id: str) -> Migration:
        module = import_module(f"snuba.migrations.{app.value}.{migration_id}")
        return module.Migration()  # type: ignore

    def _mark_completed(self, app: App, migration_id: str) -> None:
        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "app": app.value,
                "migration_id": migration_id,
                "timestamp": datetime.now(),
                "status": Status.COMPLETED.value,
                "consumer_offsets": "",
            }
        ]
        connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        connection.execute(statement, data)
