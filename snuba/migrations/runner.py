import logging

from clickhouse_driver import errors
from datetime import datetime
from functools import partial
from typing import List, Mapping, MutableMapping, NamedTuple, Tuple

from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.context import Context
from snuba.migrations.errors import (
    InvalidMigrationState,
    MigrationError,
    MigrationInProgress,
)
from snuba.migrations.groups import (
    ACTIVE_MIGRATION_GROUPS,
    get_group_loader,
    MigrationGroup,
)
from snuba.migrations.status import Status

logger = logging.getLogger("snuba.migrations")

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"


class MigrationKey(NamedTuple):
    group: MigrationGroup
    migration_id: str


class MigrationStatus(NamedTuple):
    migration_id: str
    status: Status
    blocking: bool


class Runner:
    def __init__(self) -> None:
        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        migrations_cluster = get_cluster(StorageSetKey.MIGRATIONS)
        self.__table_name = (
            LOCAL_TABLE_NAME if migrations_cluster.is_single_node() else DIST_TABLE_NAME
        )

        self.__connection = migrations_cluster.get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

    def show_all(self) -> List[Tuple[MigrationGroup, List[MigrationStatus]]]:
        """
        Returns the list of migrations and their statuses for each group.
        """
        migrations: List[Tuple[MigrationGroup, List[MigrationStatus]]] = []

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        for group in ACTIVE_MIGRATION_GROUPS:
            group_migrations: List[MigrationStatus] = []
            group_loader = get_group_loader(group)

            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                migration = group_loader.load_migration(migration_id)
                group_migrations.append(
                    MigrationStatus(
                        migration_id, get_status(migration_key), migration.blocking
                    )
                )

            migrations.append((group, group_migrations))

        return migrations

    def run_all(self, *, force: bool = False) -> None:
        """
        Run all pending migrations. Throws an error if any migration is in progress.

        Requires force to run blocking migrations.
        """
        for migration in self._get_pending_migrations():
            self.run_migration(migration, force=force)

    def _get_pending_migrations(self) -> List[MigrationKey]:
        migrations: List[MigrationKey] = []

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        for group in ACTIVE_MIGRATION_GROUPS:
            group_loader = get_group_loader(group)
            group_migrations: List[MigrationKey] = []

            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                status = get_status(migration_key)
                if status == Status.IN_PROGRESS:
                    raise MigrationInProgress(migration_key)
                if status == Status.NOT_STARTED:
                    group_migrations.append(migration_key)
                elif status == Status.COMPLETED and len(group_migrations):
                    # We should never have a completed migration after a pending one for that group
                    missing_migrations = ", ".join(
                        [m.migration_id for m in group_migrations]
                    )
                    raise InvalidMigrationState(
                        f"Missing migrations: {missing_migrations}"
                    )

            migrations.extend(group_migrations)

        return migrations

    def run_migration(
        self, migration_key: MigrationKey, *, force: bool = False
    ) -> None:
        """
        Run a single migration given its migration key and marks the migration as complete.

        Blocking migrations must be run with force.
        """
        migration_id = migration_key.migration_id

        context = Context(
            migration_id, logger, partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        if migration.blocking and not force:
            raise MigrationError("Blocking migrations must be run with force")

        migration.forwards(context)

    def reverse_migration(self, migration_key: MigrationKey) -> None:
        migration_id = migration_key.migration_id

        context = Context(
            migration_id, logger, partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        migration.backwards(context)

    def _update_migration_status(
        self, migration_key: MigrationKey, status: Status
    ) -> None:
        next_version = self._get_next_version(migration_key)

        statement = f"INSERT INTO {self.__table_name} FORMAT JSONEachRow"
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
            f"SELECT version FROM {self.__table_name} FINAL WHERE group = %(group)s AND migration_id = %(migration_id)s;",
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
        migration_groups = (
            "("
            + (
                ", ".join(
                    [escape_string(group.value) for group in ACTIVE_MIGRATION_GROUPS]
                )
            )
            + ")"
        )

        try:
            for row in self.__connection.execute(
                f"SELECT group, migration_id, status FROM {self.__table_name} FINAL WHERE group IN {migration_groups}"
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
