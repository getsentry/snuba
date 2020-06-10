from abc import ABC, abstractmethod, abstractproperty
from typing import Optional, Sequence

from snuba.migrations.operations import Operation


class Migration(ABC):
    """
    A migration consists of one or more forward operations which will be executed
    on all of the local and distributed nodes of the cluster. Upon error, the backwards
    methods will be executed. The backwards operations are responsible for returning
    the system to its pre-migration state, so that the forwards methods can be safely
    retried.

    Once the migration has been completed, we shouldn't use the backwards methods
    to try and go back to the prior state. Since migrations can delete data, attempting
    to revert cannot always bring back the previous state completely.

    The operations in a migration should bring the system from one consistent state to
    the next. There isn't a hard and fast rule about when operations should be grouped
    into a single migration vs having multiple migrations with a single operation
    each. Generally if the intermediate state between operations is not considered to
    be valid, they should be put into the same migration. If the operations are
    completely unrelated, they are probably better as separate migrations.

    Migrations that cannot be completed immediately, such as those that contain
    a data migration, must be marked with is_dangerous = True.

    The easiest way to run dangerous migrations will be with downtime. If Snuba is not
    running, we can run these migrations in the same way as non dangerous migrations.
    They may just take some time depending on the volume of data to be migrated.
    If we need to run these migrations without downtime, the migration must provide
    additional instructions for how to do this. For example if we are migrating data
    to a new table, it's likely a new consumer will need to be started in order to
    fill the new table while the old one is still filling the old table.

    If Snuba is running and we are attempting to perform a no downtime migration,
    it will not be possible to migrate forwards multiple versions past a dangerous
    migration in one go. The dangerous migration must be fully completed first,
    before the new version is downloaded and any subsequent migrations run.
    """

    @abstractproperty
    def is_dangerous(self) -> bool:
        raise NotImplementedError

    @abstractproperty
    def dependency(self) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def forwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    @abstractmethod
    def backwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    # @abstractmethod
    # def forwards_dist(self) -> Sequence[Operation]:
    #     raise NotImplementedError

    # @abstractmethod
    # def backwards_dist(self) -> Sequence[Operation]:
    #     raise NotImplementedError
