import logging

from clickhouse_driver import errors
from datetime import datetime
from enum import Enum
from typing import NamedTuple

from snuba.clickhouse.errors import ClickhouseError
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


class MigrationKey(NamedTuple):
    group: MigrationGroup
    migration_id: str

    def __str__(self) -> str:
        return f"{self.group.value} {self.migration_id}"


class Runner:
    def __init__(self) -> None:
        self.__connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

    def run_migration(self, migration_key: MigrationKey) -> None:
        """
        Run a single migration given its migration key and marks the migration as complete.
        """
        group = migration_key.group
        migration_id = migration_key.migration_id

        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        logger.info(f"Running migration: {migration_key}")

        migration = get_group_loader(group).load_migration(migration_id)

        self._update_migration_status(migration_key, Status.IN_PROGRESS)

        operations = migration.forwards_local()
        for op in operations:
            op.execute()

        logger.info(f"Finished running, updating status: {migration_key}")

        self._update_migration_status(migration_key, Status.COMPLETED)

        logger.info(f"Finished: {migration_key}")

    def _update_migration_status(
        self, migration_key: MigrationKey, status: Status
    ) -> None:
        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "group": migration_key.group.value,
                "migration_id": migration_key.migration_id,
                "timestamp": datetime.now(),
                "status": status.value,
            }
        ]

        try:
            self.__connection.execute(statement, data)
        except ClickhouseError as e:
            # HACK: Ignore failures related to non existent table when we're
            # setting system migrations to non completed statuses. This is required
            # since the migrations table might not yet exist.
            allow_fail = (
                migration_key.group == MigrationGroup.SYSTEM
                and status != Status.COMPLETED
            )

            if not allow_fail or e.code != errors.ErrorCodes.UNKNOWN_TABLE:
                raise e
