from collections import defaultdict
from datetime import datetime
from functools import partial
from typing import List, Mapping, MutableMapping, NamedTuple, Optional, Sequence, Tuple

import structlog
from clickhouse_driver import errors

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.connect import (
    check_for_inactive_replicas,
    get_clickhouse_clusters_for_migration_group,
    get_column_states,
)
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
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.status import Status

logger = structlog.get_logger().bind(module=__name__)

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"


def get_active_migration_groups() -> Sequence[MigrationGroup]:
    groups = []
    for group in MigrationGroup:
        if (
            group in OPTIONAL_GROUPS
            and group.value in settings.SKIPPED_MIGRATION_GROUPS
            or get_group_readiness_state(group).value not in settings.SUPPORTED_STATES
        ):
            continue
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
    exists: bool


class Runner:
    def __init__(self) -> None:
        self.__migrations_cluster = get_cluster(StorageSetKey.MIGRATIONS)
        self.__table_name = (
            LOCAL_TABLE_NAME
            if self.__migrations_cluster.is_single_node()
            else DIST_TABLE_NAME
        )

        self.__connection = self.__migrations_cluster.get_query_connection(
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

    def force_overwrite_status(
        self, group: MigrationGroup, migration_id: str, new_status: Status
    ) -> None:
        """Sometimes a migration gets blocked or times out for whatever reason.
        This function is used to overwrite the state in the snuba table keeping
        track of migration so we can try again"""
        local_node = self.__migrations_cluster.get_local_nodes()[0]
        local_node_connection = self.__migrations_cluster.get_node_connection(
            ClickhouseClientSettings.MIGRATE, local_node
        )

        local_node_connection.execute(
            f"ALTER TABLE {LOCAL_TABLE_NAME} UPDATE status='{new_status.value}' WHERE migration_id='{migration_id}'"
        )

    def show_all(
        self, groups: Optional[Sequence[str]] = None, include_nonexistent: bool = False
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
        clickhouse_group_migrations = defaultdict(set)
        for group, migration_id in migration_status.keys():
            clickhouse_group_migrations[group].add(migration_id)

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        for group in migration_groups:
            group_migrations: List[MigrationDetails] = []
            group_loader = get_group_loader(group)

            migration_ids = group_loader.get_migrations()
            for migration_id in migration_ids:
                migration_key = MigrationKey(group, migration_id)
                migration = group_loader.load_migration(migration_id)
                group_migrations.append(
                    MigrationDetails(
                        migration_id,
                        get_status(migration_key),
                        migration.blocking,
                        True,
                    )
                )

            if include_nonexistent:
                non_existing_migrations = clickhouse_group_migrations.get(
                    group, set()
                ).difference(set(migration_ids))
                for migration_id in non_existing_migrations:
                    migration_key = MigrationKey(group, migration_id)
                    group_migrations.append(
                        MigrationDetails(
                            migration_id, get_status(migration_key), False, False
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
        check_dangerous: bool = False,
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
                self._run_migration_impl(
                    migration_key, force=force, check_dangerous=check_dangerous
                )

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
        check_dangerous: bool = False,
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
            self._run_migration_impl(
                migration_key, dry_run=True, check_dangerous=check_dangerous
            )
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
            self._run_migration_impl(
                migration_key, force=force, check_dangerous=check_dangerous
            )

    def _run_migration_impl(
        self,
        migration_key: MigrationKey,
        *,
        force: bool = False,
        dry_run: bool = False,
        check_dangerous: bool = False,
    ) -> None:
        migration_id = migration_key.migration_id

        if not dry_run:
            check_for_inactive_replicas(
                get_clickhouse_clusters_for_migration_group(migration_key.group)
            )

        context = Context(
            migration_id,
            logger,
            partial(self._update_migration_status, migration_key),
        )
        migration = get_group_loader(migration_key.group).load_migration(migration_id)

        if migration.blocking and not dry_run and not force:
            raise MigrationError("Blocking migrations must be run with force")

        columns_states = get_column_states() if check_dangerous else None
        if isinstance(migration, ClickhouseNodeMigration):
            migration.forwards(context, dry_run, columns_states)
        else:
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

    def reverse_all(
        self,
        *,
        through: str = "all",
        fake: bool = False,
        force: bool = False,
        include_system: bool = False,
        group: Optional[MigrationGroup] = None,
        readiness_states: Optional[Sequence[ReadinessState]] = None,
    ) -> None:

        groups = (
            [group]
            if group
            else (
                get_active_migration_groups()
                if include_system
                else [
                    g
                    for g in get_active_migration_groups()
                    if g != MigrationGroup.SYSTEM
                ]
            )
        )
        completed_migrations = self._get_completed_migrations(groups)

        if readiness_states:
            completed_migrations = [
                m
                for m in completed_migrations
                if get_group_readiness_state(m.group) in readiness_states
            ]

        use_through = False if through == "all" else True

        def exact_migration_exists(through: str) -> bool:
            migration_ids = [
                key.migration_id
                for key in completed_migrations
                if key.migration_id.startswith(through)
            ]
            if len(migration_ids) == 1:
                return True
            return False

        if use_through and not exact_migration_exists(through):
            raise MigrationError(f"No exact match for: {through}")

        if not force:
            raise MigrationError("Requires force to reverse migrations")

        for migration_key in completed_migrations:
            if fake:
                self._update_migration_status(migration_key, Status.NOT_STARTED)
            else:
                self._reverse_migration_impl(migration_key)

            if use_through and migration_key.migration_id.startswith(through):
                logger.info(f"Reverse through: {migration_key.migration_id}")
                break

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

        if not dry_run:
            check_for_inactive_replicas(
                get_clickhouse_clusters_for_migration_group(migration_key.group)
            )

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

    def _get_completed_migrations(
        self, groups: List[MigrationGroup]
    ) -> List[MigrationKey]:
        """
        Get a list of completed migrations for a list of groups
        """
        migration_status = self._get_migration_status()

        def get_status(migration_key: MigrationKey) -> Status:
            return migration_status.get(migration_key, Status.NOT_STARTED)

        group_migrations: List[MigrationKey] = []
        for group in groups:
            group_loader = get_group_loader(group)
            for migration_id in group_loader.get_migrations():
                migration_key = MigrationKey(group, migration_id)
                status = get_status(migration_key)
                if status == Status.IN_PROGRESS:
                    # can't reverse migrations if one is stuck pending
                    raise MigrationInProgress(str(migration_key))
                if status == Status.NOT_STARTED:
                    continue
                elif status == Status.COMPLETED:
                    group_migrations.append(migration_key)
        # need opposite order
        group_migrations.reverse()
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
