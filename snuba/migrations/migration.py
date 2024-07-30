import warnings
from abc import ABC, abstractmethod, abstractproperty
from typing import Optional, Sequence

from snuba.clusters.cluster import get_cluster
from snuba.migrations.check_dangerous import check_dangerous_operation
from snuba.migrations.context import Context
from snuba.migrations.operations import GenericOperation, OperationTarget, SqlOperation
from snuba.migrations.status import Status
from snuba.utils.types import ColumnStatesMapType


class Migration(ABC):
    """
    A Migration should implement the forwards and backwards methods. Migrations should
    not use this class directly, rather they should extend either the ClickHouseNodeMigration
    (for SQL migrations to be run on ClickHouse) or CodeMigration (for Python migrations).
    SquashedMigration can be used for a noop migration.

    Migrations that cannot be completed immediately, such as those that contain
    a data migration, must be marked with blocking = True.

    The easiest way to run blocking migrations will be with downtime. If Snuba is not
    running, we can run these migrations in the same way as non blocking migrations.
    They may just take some time depending on the volume of data to be migrated.
    If we need to run these migrations without downtime, the migration must provide
    additional instructions for how to do this. For example if we are migrating data
    to a new table, it's likely a new consumer will need to be started in order to
    fill the new table while the old one is still filling the old table.

    If Snuba is running and we are attempting to perform a no downtime migration,
    it will not be possible to migrate forwards multiple versions past a blocking
    migration in one go. The blocking migration must be fully completed first,
    before the new version is downloaded and any subsequent migrations run.
    """

    def is_first_migration(self) -> bool:
        return False

    @abstractproperty
    def blocking(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def forwards(self, context: Context, dry_run: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def backwards(self, context: Context, dry_run: bool) -> None:
        raise NotImplementedError


class SquashedMigration(Migration):
    """
    This is a migration that used to exist. We keep the migration ID in the DB
    sequence but it no longer does anything as it is safe to no longer run.
    """

    blocking = False

    def forwards(self, context: Context, dry_run: bool) -> None:
        _migration_id, _logger, update_status = context
        update_status(Status.COMPLETED)

    def backwards(self, context: Context, dry_run: bool) -> None:
        _migration_id, _logger, update_status = context
        update_status(Status.NOT_STARTED)


class CodeMigration(Migration, ABC):
    """
    Consists of one or more Python functions executed in sequence.
    """

    @abstractmethod
    def forwards_global(self) -> Sequence[GenericOperation]:
        raise NotImplementedError

    @abstractmethod
    def backwards_global(self) -> Sequence[GenericOperation]:
        raise NotImplementedError

    def forwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            for op in self.forwards_global():
                desc = op.description() or "No description provided"
                print(f"Non SQL operation - {desc}")
            return

        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        update_status(Status.IN_PROGRESS)

        for op in self.forwards_global():
            op.execute(logger)

        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            for op in self.backwards_global():
                desc = op.description() or "No description provided"
                print(f"Non SQL operation - {desc}")
            return

        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        update_status(Status.IN_PROGRESS)
        for op in self.backwards_global():
            op.execute(logger)
        logger.info(f"Finished reversing: {migration_id}")

        update_status(Status.NOT_STARTED)


class ClickhouseNodeMigration(Migration, ABC):
    """
    A ClickhouseNodeMigration consists of one or more forward operations which will be executed
    on all of the local and distributed nodes of the cluster. Upon error, the backwards
    methods can be executed to restore the state. The backwards operations are responsible
    for returning the system to its pre-migration state, so that the forwards methods can be
    safely retried.

    Once the migration has been completed, we shouldn't use the backwards methods
    to try and go back to the prior state. Since migrations can delete data, attempting
    to revert cannot always bring back the previous state completely.

    The operations in a migration should bring the system from one consistent state to
    the next. There isn't a hard and fast rule about when operations should be grouped
    into a single migration vs having multiple migrations with a single operation
    each. Generally if the intermediate state between operations is not considered to
    be valid, they should be put into the same migration. If the operations are
    completely unrelated, they are probably better as separate migrations.
    """

    @abstractmethod
    def backwards_ops(self) -> Sequence[SqlOperation]:
        raise NotImplementedError()

    @abstractmethod
    def forwards_ops(self) -> Sequence[SqlOperation]:
        raise NotImplementedError()

    def forwards(
        self,
        context: Context,
        dry_run: bool = False,
        columns_state_to_check: Optional[ColumnStatesMapType] = None,
    ) -> None:
        ops = self.forwards_ops()

        if columns_state_to_check and not self.blocking:
            for op in ops:
                check_dangerous_operation(op, columns_state_to_check)

        if dry_run:
            self.__dry_run(ops)
            return

        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")

        # The table does not exist before the first migration is run
        # so do not update status yet
        if not self.is_first_migration():
            update_status(Status.IN_PROGRESS)

        for op in ops:
            op.execute()

        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(
        self,
        context: Context,
        dry_run: bool,
        columns_state_to_check: Optional[ColumnStatesMapType] = None,
    ) -> None:
        ops = self.backwards_ops()
        if dry_run:
            self.__dry_run(ops)
            return

        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        update_status(Status.IN_PROGRESS)

        for op in ops:
            if columns_state_to_check:
                check_dangerous_operation(op, columns_state_to_check)
            op.execute()
        logger.info(f"Finished reversing: {migration_id}")

        # The migrations table will be destroyed if the first
        # migration is reversed; do not attempt to update status
        if not self.is_first_migration():
            update_status(Status.NOT_STARTED)

    def __dry_run(
        self,
        ops: Sequence[SqlOperation],
    ) -> None:
        def print_dist_op(op: SqlOperation) -> None:
            cluster = get_cluster(op._storage_set)
            is_single_node = cluster.is_single_node()
            if not is_single_node:
                print(f"Distributed op: {op.format_sql()}")
            else:
                print("Skipped dist operation - single node cluster")

        for op in ops:
            if op.target == OperationTarget.LOCAL:
                print(f"Local op: {op.format_sql()}")
            elif op.target == OperationTarget.DISTRIBUTED:
                print_dist_op(op)
            else:
                print("Skipped operation - no target specified")


class ClickhouseNodeMigrationLegacy(ClickhouseNodeMigration, ABC):
    """
    This is now deprecated. Use ClickhouseNodeMigration instead.
    """

    forwards_local_first: bool = True
    backwards_local_first: bool = False

    @abstractmethod
    def forwards_local(self) -> Sequence[SqlOperation]:
        raise NotImplementedError

    @abstractmethod
    def backwards_local(self) -> Sequence[SqlOperation]:
        raise NotImplementedError

    @abstractmethod
    def forwards_dist(self) -> Sequence[SqlOperation]:
        raise NotImplementedError

    @abstractmethod
    def backwards_dist(self) -> Sequence[SqlOperation]:
        raise NotImplementedError

    def _set_targets(
        self, ops: Sequence[SqlOperation], target: OperationTarget
    ) -> None:
        """For old migrations using the old methods, set the target appropriately."""
        for op in ops:
            op.target = target

    def backwards_ops(self) -> Sequence[SqlOperation]:
        warnings.warn(
            "backwards_local and backwards_dist are deprecated. Use backwards_ops instead.",
            DeprecationWarning,
        )
        local_ops, dist_ops = self.backwards_local(), self.backwards_dist()
        self._set_targets(local_ops, OperationTarget.LOCAL)
        self._set_targets(dist_ops, OperationTarget.DISTRIBUTED)

        if self.backwards_local_first:
            return (*local_ops, *dist_ops)
        else:
            return (*dist_ops, *local_ops)

    def forwards_ops(self) -> Sequence[SqlOperation]:
        warnings.warn(
            "forwards_local and forwards_dist are deprecated. Use forwards_ops instead.",
            DeprecationWarning,
        )
        local_ops, dist_ops = self.forwards_local(), self.forwards_dist()
        self._set_targets(local_ops, OperationTarget.LOCAL)
        self._set_targets(dist_ops, OperationTarget.DISTRIBUTED)

        if self.forwards_local_first:
            return (*local_ops, *dist_ops)
        else:
            return (*dist_ops, *local_ops)
