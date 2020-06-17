import logging

from clickhouse_driver import errors
from datetime import datetime
from enum import Enum
from typing import List, MutableMapping, NamedTuple, Optional

from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.errors import MigrationInProgress
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


class Runner:
    def __init__(self) -> None:
        self.__connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        self.__migrations_status: Optional[MutableMapping[MigrationKey, Status]] = None

    def run_all(self) -> None:
        """
        Run all pending migrations. Throws an error if any migration is in progress.
        """
        for migration in self.get_pending_migrations():
            self.run_migration(migration)

    def get_pending_migrations(self) -> List[MigrationKey]:
        migrations: List[MigrationKey] = []

        for group in MigrationGroup:
            group_loader = get_group_loader(group)

            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                status = self._get_migration_status(migration_key)
                if status == Status.IN_PROGRESS:
                    raise MigrationInProgress(migration_key)
                if status == Status.NOT_STARTED:
                    migrations.append(migration_key)

        return migrations

    def run_migration(self, migration_key: MigrationKey) -> None:
        """
        Run a single migration given its group and ID.
        Marks the migration as completed.
        """
        group = migration_key.group
        migration_id = migration_key.migration_id

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
        self._mark_completed(migration_key)

        logger.info(f"Finished: {group} {migration_id}")

    def _mark_completed(self, migration_key: MigrationKey) -> None:
        status = Status.COMPLETED

        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "group": migration_key.group.value,
                "migration_id": migration_key.migration_id,
                "timestamp": datetime.now(),
                "status": status.value,
                # TODO: Version should be incremented each time we update that
                # migration status
                "version": 1,
            }
        ]
        connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        connection.execute(statement, data)

        if self.__migrations_status is not None:
            self.__migrations_status[migration_key] = status

    def _get_migration_status(self, migration_key: MigrationKey) -> Status:
        if self.__migrations_status is None:
            data: MutableMapping[MigrationKey, Status] = {}

            try:
                data = {}
                for row in self.__connection.execute(
                    f"SELECT group, migration_id, status FROM {TABLE_NAME} FINAL;"
                ):
                    group_name, migration_id, status_name = row
                    data[
                        MigrationKey(MigrationGroup(group_name), migration_id)
                    ] = Status(status_name)
                self.__migrations_status = data
            except ClickhouseError as e:
                # If the table wasn't created yet, no migrations have started yet.
                if e.code == errors.ErrorCodes.UNKNOWN_TABLE:
                    self.__migrations_status = {}
                else:
                    raise e

        return self.__migrations_status.get(migration_key, Status.NOT_STARTED)
