from abc import ABC, abstractmethod
from enum import Enum
from typing import NamedTuple, Optional, Sequence

from snuba.migrations.operations import Operation


class App(Enum):
    SYSTEM = "system"
    SNUBA = "snuba"


class Dependency(NamedTuple):
    app: App
    migration_id: str


class Migration(ABC):
    # Set is_dangerous to True when a migration cannot be immediately completed,
    # such as when it contains a data migration operation or some other manual
    # step is required.
    # Migrations marked as dangerous are considered blocking. Subsequent versions
    # should not be run until the dangerous migration is completed.
    is_dangerous: bool
    dependencies: Optional[Sequence[Dependency]]

    @abstractmethod
    def forwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    @abstractmethod
    def backwards_local(self) -> Sequence[Operation]:
        raise NotImplementedError

    # @abstractmethod
    def forwards_dist(self) -> Sequence[Operation]:
        raise NotImplementedError

    # @abstractmethod
    def backwards_dist(self) -> Sequence[Operation]:
        raise NotImplementedError
