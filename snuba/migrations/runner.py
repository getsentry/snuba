from datetime import datetime
from functools import partial
from typing import List, Mapping, MutableMapping, NamedTuple, Optional, Sequence, Tuple

import structlog
from clickhouse_driver import errors

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseNodeType,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.context import Context
from snuba.migrations.errors import (
    InvalidMigrationState,
    MigrationError,
    MigrationInProgress,
)
from snuba.migrations.groups import (
    OPTIONAL_GROUPS,
    MigrationGroup,
    get_group_loader,
    get_group_readiness_state,
)
from snuba.migrations.migration import ClickhouseNodeMigration, CodeMigration, Migration
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.migrations.status import Status

logger = structlog.get_logger().bind(module=__name__)

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"


def get_active_migration_groups() -> Sequence[MigrationGroup]:
    groups = []
    for group in MigrationGroup:
        if group.value in settings.READINESS_STATE_MIGRATION_GROUPS_ENABLED:
            readiness_state = get_group_readiness_state(group)
            if readiness_state.value in settings.SUPPORTED_STATES:
                groups.append(group)
        else:
            if not (
                group in OPTIONAL_GROUPS
                and group.value in settings.SKIPPED_MIGRATION_GROUPS
            ):
                groups.append(group)
    return groups


class MigrationKey(NamedTuple):
    group: MigrationGroup
    migration_id: str

    def __str__(self) -> str:
        return f"{self.group.value}: {self.migration_id}"


class MigrationDetails(NamedTuple):
    migration_id: str
    status: Status
    blocking: bool


class Runner:
    def __init__(self) -> None:
        migrations_cluster = get_cluster(StorageSetKey.MIGRATIONS)
        self.__table_name = (
            LOCAL_TABLE_NAME if migrations_cluster.is_single_node() else DIST_TABLE_NAME
        )

        self.__connection = migrations_cluster.get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        self.__status: MutableMapping[
            MigrationKey, Tuple[Status, Optional[datetime]]
        ] = {}

    def get_status(
        self, migration_key: MigrationKey
    ) -> Tuple[Status, Optional[datetime]]:
        """
        Returns the status and timestamp of a migration.
        """

        if migration_key in self.__status:
            return self.__status[migration_key]

        try:
            data = self.__connection.execute(
                f"SELECT status, timestamp FROM {self.__table_name} FINAL WHERE group = %(group)s AND migration_id = %(migration_id)s",
                {
                    "group": migration_key.group.value,
                    "migration_id": migration_key.migration_id,
                },
            ).results

            if data:
                status, timestamp = data[0]
                self.__status[migration_key] = (Status(status), timestamp)
            else:
                self.__status[migration_key] = (Status.NOT_STARTED, None)

            return self.__status[migration_key]

        except ClickhouseError as e:
            # If the table wasn't created yet, no migrations have started.
            if e.code != errors.ErrorCodes.UNKNOWN_TABLE:
                raise e

        return Status.NOT_STARTED, None

    def show_all(
        self, groups: Optional[Sequence[str]] = None
    ) -> List[Tuple[MigrationGroup, List[MigrationDetails]]]:
        """
        Returns the list of migrations and their statuses for each group.
        """
        migrations: List[Tuple[MigrationGroup, List[MigrationDetails]]] = []

        if groups:
            migration_groups: Sequence[MigrationGroup] = [
                MigrationGroup(group) for group in groups
            ]
        else:
            migration_groups = get_active_migration_groups()

        migration_status = self._get_migration_status(migration_groups)

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        for group in migration_groups:
            group_migrations: List[MigrationDetails] = []
            group_loader = get_group_loader(group)

            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                migration = group_loader.load_migration(migration_id)
                group_migrations.append(
                    MigrationDetails(
                        migration_id, get_status(migration_key), migration.blocking
                    )
                )

            migrations.append((group, group_migrations))

        return migrations

    def run_all(
        self,
        *,
        through: str = "all",
        fake: bool = False,
        force: bool = False,
        group: Optional[MigrationGroup] = None,
        readiness_states: Optional[Sequence[ReadinessState]] = None,
    ) -> None:
        """
        If group is specified, runs all pending migrations for that specific group. Makes
        sure to run any pending system migrations first so that the migrations table is
        created before running migrations for other groups. Throw an error if any migration
        is in progress.

        If no group is specified, run all pending migrations. Throws an error if any
        migration is in progress.

        Requires force to run blocking migrations.
        """

        if not group:
            pending_migrations = self._get_pending_migrations()
        else:
            pending_migrations = self._get_pending_migrations_for_group(
                MigrationGroup.SYSTEM
            ) + self._get_pending_migrations_for_group(group)

        if readiness_states:
            pending_migrations = [
                m
                for m in pending_migrations
                if get_group_readiness_state(m.group) in readiness_states
            ]

        use_through = False if through == "all" else True

        def exact_migration_exists(through: str) -> bool:
            migration_ids = [
                key.migration_id
                for key in pending_migrations
                if key.migration_id.startswith(through)
            ]
            if len(migration_ids) == 1:
                return True
            return False

        if use_through and not exact_migration_exists(through):
            raise MigrationError(f"No exact match for: {through}")

        # Do not run migrations if any are blocking
        if not force:
            for migration_key in pending_migrations:
                migration = get_group_loader(migration_key.group).load_migration(
                    migration_key.migration_id
                )
                if migration.blocking:
                    raise MigrationError("Requires force to run blocking migrations")

        for migration_key in pending_migrations:
            if fake:
                self._update_migration_status(migration_key, Status.COMPLETED)
            else:
                self._run_migration_impl(migration_key, force=force)

            if use_through and migration_key.migration_id.startswith(through):
                logger.info(f"Ran through: {migration_key.migration_id}")
                break

    def run_migration(
        self,
        migration_key: MigrationKey,
        *,
        force: bool = False,
        fake: bool = False,
        dry_run: bool = False,
    ) -> None:
        """
        Run a single migration given its migration key and marks the migration as complete.

        Blocking migrations must be run with force.
        """

        migration_group, migration_id = migration_key

        group_migrations = get_group_loader(migration_group).get_migrations()

        if migration_id not in group_migrations:
            raise MigrationError("Could not find migration in group")

        if dry_run:
            self._run_migration_impl(migration_key, dry_run=True)
            return

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        if get_status(migration_key) != Status.NOT_STARTED:
            status_text = get_status(migration_key).value
            raise MigrationError(f"Migration is already {status_text}")

        for m in group_migrations[: group_migrations.index(migration_id)]:
            if get_status(MigrationKey(migration_group, m)) != Status.COMPLETED:
                raise MigrationError("Earlier migrations ned to be completed first")

        if fake:
            self._update_migration_status(migration_key, Status.COMPLETED)
        else:
            self._run_migration_impl(migration_key, force=force)

    def _run_migration_impl(
        self, migration_key: MigrationKey, *, force: bool = False, dry_run: bool = False
    ) -> None:
        migration_id = migration_key.migration_id

        context = Context(
            migration_id,
            logger,
            partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        if migration.blocking and not dry_run and not force:
            raise MigrationError("Blocking migrations must be run with force")

        migration.forwards(context, dry_run)

    def reverse_migration(
        self,
        migration_key: MigrationKey,
        *,
        force: bool = False,
        fake: bool = False,
        dry_run: bool = False,
    ) -> None:
        """
        Reverses a migration.
        """

        migration_group, migration_id = migration_key

        group_migrations = get_group_loader(migration_group).get_migrations()

        if migration_id not in group_migrations:
            raise MigrationError("Invalid migration")

        if dry_run:
            self._reverse_migration_impl(migration_key, dry_run=True)
            return

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        if get_status(migration_key) == Status.NOT_STARTED:
            raise MigrationError("You cannot reverse a migration that has not been run")

        if get_status(migration_key) == Status.COMPLETED and not force and not fake:
            raise MigrationError(
                "You must use force to revert an already completed migration"
            )

        for m in group_migrations[group_migrations.index(migration_id) + 1 :]:
            if get_status(MigrationKey(migration_group, m)) != Status.NOT_STARTED:
                raise MigrationError("Subsequent migrations must be reversed first")

        if fake:
            self._update_migration_status(migration_key, Status.NOT_STARTED)
        else:
            self._reverse_migration_impl(migration_key)

    def reverse_in_progress(
        self,
        fake: bool = False,
        group: Optional[MigrationGroup] = None,
        dry_run: bool = False,
    ) -> None:
        """
        Reverse migrations that are stuck in progress. This can be done
        for all migrations groups or for a select migration group. There
        will be at most one in-progress migration per group since you
        can't move forward if a migration is in-progress.
        """

        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        if group:
            migration_groups: Sequence[MigrationGroup] = [group]
        else:
            migration_groups = get_active_migration_groups()

        def get_in_progress_migration(group: MigrationGroup) -> Optional[MigrationKey]:
            group_migrations = get_group_loader(group).get_migrations()
            for migration_id in group_migrations:
                migration_key = MigrationKey(group, migration_id)
                status = get_status(migration_key)
                if status == Status.IN_PROGRESS:
                    return migration_key
            return None

        for group in migration_groups:
            migration_key = get_in_progress_migration(group)
            if migration_key:
                if fake:
                    self._update_migration_status(migration_key, Status.NOT_STARTED)
                else:
                    logger.info(f"[{group}]: reversing {migration_key}")
                    self._reverse_migration_impl(migration_key, dry_run=dry_run)
            else:
                logger.info(f"[{group}]: no in progress migrations found")

    def _reverse_migration_impl(
        self, migration_key: MigrationKey, *, dry_run: bool = False
    ) -> None:
        migration_id = migration_key.migration_id

        context = Context(
            migration_id,
            logger,
            partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        migration.backwards(context, dry_run)

    def _get_pending_migrations(self) -> List[MigrationKey]:
        """
        Gets pending migration list.
        """
        migrations: List[MigrationKey] = []

        for group in get_active_migration_groups():
            group_migrations = self._get_pending_migrations_for_group(group)
            migrations.extend(group_migrations)

        return migrations

    def _get_pending_migrations_for_group(
        self, group: MigrationGroup
    ) -> List[MigrationKey]:
        """
        Gets pending migrations list for a specific group
        """
        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        group_loader = get_group_loader(group)
        group_migrations: List[MigrationKey] = []

        for migration_id in group_loader.get_migrations():
            migration_key = MigrationKey(group, migration_id)
            status = get_status(migration_key)
            if status == Status.IN_PROGRESS:
                raise MigrationInProgress(str(migration_key))
            if status == Status.NOT_STARTED:
                group_migrations.append(migration_key)
            elif status == Status.COMPLETED and len(group_migrations):
                # We should never have a completed migration after a pending one for that group
                missing_migrations = ", ".join(
                    [m.migration_id for m in group_migrations]
                )
                raise InvalidMigrationState(f"Missing migrations: {missing_migrations}")

        return group_migrations

    def _update_migration_status(
        self, migration_key: MigrationKey, status: Status
    ) -> None:
        self.__status = {}
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
        ).results
        if result:
            (version,) = result[0]
            return int(version) + 1

        return 1

    def _get_migration_status(
        self, groups: Optional[Sequence[MigrationGroup]] = None
    ) -> Mapping[MigrationKey, Status]:
        data: MutableMapping[MigrationKey, Status] = {}

        if not groups:
            groups = get_active_migration_groups()

        migration_groups = (
            "("
            + (
                ", ".join(
                    [
                        escape_string(group.value)
                        for group in get_active_migration_groups()
                    ]
                )
            )
            + ")"
        )

        try:
            for row in self.__connection.execute(
                f"SELECT group, migration_id, status FROM {self.__table_name} FINAL WHERE group IN {migration_groups}"
            ).results:
                group_name, migration_id, status_name = row
                data[MigrationKey(MigrationGroup(group_name), migration_id)] = Status(
                    status_name
                )
        except ClickhouseError as e:
            # If the table wasn't created yet, no migrations have started.
            if e.code != errors.ErrorCodes.UNKNOWN_TABLE:
                raise e

        return data

    @classmethod
    def add_node(
        self,
        node_type: ClickhouseNodeType,
        storage_sets: Sequence[StorageSetKey],
        host_name: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        client_settings = ClickhouseClientSettings.MIGRATE.value
        clickhouse = ClickhousePool(
            host_name,
            port,
            user,
            password,
            database,
            client_settings=client_settings.settings,
            send_receive_timeout=client_settings.timeout,
        )

        migrations: List[Migration] = []

        for group in get_active_migration_groups():
            group_loader = get_group_loader(group)

            for migration_id in group_loader.get_migrations():
                migration = group_loader.load_migration(migration_id)
                migrations.append(migration)

        for migration in migrations:
            if isinstance(migration, ClickhouseNodeMigration):
                operations = [
                    op
                    for op in migration.forwards_ops()
                    if (
                        op.target == OperationTarget.LOCAL
                        if node_type == ClickhouseNodeType.LOCAL
                        else op.target == OperationTarget.DISTRIBUTED
                    )
                ]

                for sql_op in operations:
                    if isinstance(sql_op, SqlOperation):
                        if sql_op._storage_set in storage_sets:
                            sql = sql_op.format_sql()
                            logger.info(f"Executing {sql}")
                            clickhouse.execute(sql)
            elif isinstance(migration, CodeMigration):
                for python_op in migration.forwards_global():
                    python_op.execute_new_node(storage_sets)
