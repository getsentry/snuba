from abc import ABC, abstractmethod, abstractproperty
from typing import Sequence

from snuba.clusters.cluster import get_cluster
from snuba.migrations.context import Context
from snuba.migrations.operations import RunPython, SqlOperation
from snuba.migrations.status import Status


class Migration(ABC):
    """
    A Migration should implement the forwards and backwards methods. Migrations should
    not use this class directly, rather they should extend either the ClickHouseNodeMigration
    (for SQL migrations to be run on ClickHouse) or GlobalMigration (for Python migrations).

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


class GlobalMigration(Migration, ABC):
    """
    Consists of one or more Python functions executed once globally.
    """

    @abstractmethod
    def forwards_global(self) -> Sequence[RunPython]:
        raise NotImplementedError

    @abstractmethod
    def backwards_global(self) -> Sequence[RunPython]:
        raise NotImplementedError

    def forwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            print("Non SQL operation")
            return

        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        if not self.is_first_migration():
            update_status(Status.IN_PROGRESS)

        for op in self.forwards_global():
            op.execute()

        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            print("Non SQL operation")
            return

        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        update_status(Status.IN_PROGRESS)
        for op in self.backwards_global():
            op.execute()
        logger.info(f"Finished reversing: {migration_id}")

        # The migrations table will be destroyed if the first
        # migration is reversed; do not attempt to update status
        if not self.is_first_migration():
            update_status(Status.NOT_STARTED)


class ClickhouseNodeMigration(Migration, ABC):
    """
    A MultiStepMigration consists of one or more forward operations which will be executed
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

    def forwards(self, context: Context, dry_run: bool = False) -> None:
        if dry_run:
            self.__dry_run(self.forwards_local(), self.forwards_dist())
            return

        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")

        # The table does not exist before the first migration is run
        # so do not update status yet
        if not self.is_first_migration():
            update_status(Status.IN_PROGRESS)
        for op in self.forwards_local():
            op.execute(local=True)
        for op in self.forwards_dist():
            op.execute(local=False)
        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            self.__dry_run(self.backwards_local(), self.backwards_dist())
            return

        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        update_status(Status.IN_PROGRESS)
        for op in self.backwards_dist():
            op.execute(local=False)
        for op in self.backwards_local():
            op.execute(local=True)
        logger.info(f"Finished reversing: {migration_id}")

        # The migrations table will be destroyed if the first
        # migration is reversed; do not attempt to update status
        if not self.is_first_migration():
            update_status(Status.NOT_STARTED)

    def __dry_run(
        self,
        local_operations: Sequence[SqlOperation],
        dist_operations: Sequence[SqlOperation],
    ) -> None:

        print("Local operations:")
        if len(local_operations) == 0:
            print("n/a")

        for op in local_operations:
            print(op.format_sql())

        print("\n")
        print("Dist operations:")

        if len(dist_operations) == 0:
            print("n/a")

        for op in dist_operations:
            cluster = get_cluster(op._storage_set)

            if not cluster.is_single_node():
                print(op.format_sql())
            else:
                print("Skipped dist operation - single node cluster")
