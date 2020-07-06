import logging

from clickhouse_driver import errors
from datetime import datetime
from functools import partial
from typing import List, Mapping, MutableMapping, NamedTuple, Tuple

from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.context import Context
from snuba.migrations.errors import InvalidMigrationState, MigrationInProgress
from snuba.migrations.groups import get_group_loader, MigrationGroup
from snuba.migrations.status import Status

logger = logging.getLogger("snuba.migrations")

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"

# Currently only local migrations are supported
TABLE_NAME = LOCAL_TABLE_NAME


class MigrationKey(NamedTuple):
    group: MigrationGroup
    migration_id: str


class Runner:
    def __init__(self) -> None:
        self.__connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

    def run_all(self) -> None:
        """
        Run all pending migrations. Throws an error if any migration is in progress.

        Before new migrations are run, any previously completed migrations that
        are now marked deprecated are reversed.
        """
        migrations_to_reverse, migrations_to_run = self._get_pending_migrations()

        for migration in reversed(migrations_to_reverse):
            self.run_migration(migration, reverse=True)

        for migration in migrations_to_run:
            self.run_migration(migration)

    def _get_pending_migrations(self) -> Tuple[List[MigrationKey], List[MigrationKey]]:
        migrations_to_reverse: List[MigrationKey] = []
        migrations_to_run: List[MigrationKey] = []

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        for group in MigrationGroup:
            group_loader = get_group_loader(group)
            group_migrations: List[MigrationKey] = []

            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                migration = group_loader.load_migration(migration_id)
                status = get_status(migration_key)

                if status == Status.IN_PROGRESS:
                    raise MigrationInProgress(migration_key)

                if status == Status.NOT_STARTED and migration.deprecated == False:
                    group_migrations.append(migration_key)
                elif status == Status.COMPLETED and len(group_migrations):
                    # We should never have a completed migration after a pending one for that group
                    missing_migrations = ", ".join(
                        [m.migration_id for m in group_migrations]
                    )
                    raise InvalidMigrationState(
                        f"Missing migrations: {missing_migrations}"
                    )
                elif status == Status.COMPLETED and migration.deprecated == True:
                    migrations_to_reverse.append(migration_key)

            migrations_to_run.extend(group_migrations)

        return migrations_to_reverse, migrations_to_run

    def run_migration(self, migration_key: MigrationKey, reverse: bool = False) -> None:
        """
        Run a single migration given its migration key and marks the migration as complete.
        """
        migration_id = migration_key.migration_id

        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        context = Context(
            migration_id, logger, partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        if reverse:
            migration.backwards(context)
        else:
            migration.forwards(context)

    def _update_migration_status(
        self, migration_key: MigrationKey, status: Status
    ) -> None:
        next_version = self._get_next_version(migration_key)

        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "group": migration_key.group.value,
                "migration_id": migration_key.migration_id,
                "timestamp": datetime.now(),
                "status": status.value,
                "version": next_version,
            }
        ]
        self.__connection.execute(statement, data)

    def _get_next_version(self, migration_key: MigrationKey) -> int:
        result = self.__connection.execute(
            f"SELECT version FROM {TABLE_NAME} FINAL WHERE group = %(group)s AND migration_id = %(migration_id)s;",
            {
                "group": migration_key.group.value,
                "migration_id": migration_key.migration_id,
            },
        )
        if result:
            (version,) = result[0]
            return int(version) + 1

        return 1

    def _get_migration_status(self) -> Mapping[MigrationKey, Status]:
        data: MutableMapping[MigrationKey, Status] = {}

        try:
            for row in self.__connection.execute(
                f"SELECT group, migration_id, status FROM {TABLE_NAME} FINAL"
            ):
                group_name, migration_id, status_name = row
                data[MigrationKey(MigrationGroup(group_name), migration_id)] = Status(
                    status_name
                )
        except ClickhouseError as e:
            # If the table wasn't created yet, no migrations have started.
            if e.code != errors.ErrorCodes.UNKNOWN_TABLE:
                raise e

        return data
