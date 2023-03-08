from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Optional, Sequence, Set, Type, TypeVar

from snuba import settings


class Category(Enum):
    MIGRATIONS = "migrations"


class Resource(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    @abstractproperty
    def category(self) -> Category:
        raise NotImplementedError


class MigrationResource(Resource):
    @property
    def category(self) -> Category:
        return Category.MIGRATIONS


TResource = TypeVar("TResource", bound=Resource)


class Action(ABC, Generic[TResource]):
    """
    An action is used to describe the permissions a user has
    on a specific set of resources.
    """

    @abstractmethod
    def validated_resources(
        self, resources: Sequence[TResource]
    ) -> Sequence[TResource]:
        """
        A resource is considered valid if the action can be
        taken on the resource.

        e.g. a user can "execute" (action) a migration within
        a "migration group" (resource)

        Raise an error if any resources are invalid, otherwise
        return the resources.
        """
        raise NotImplementedError


class MigrationAction(Action[MigrationResource]):
    def __init__(self, resources: Sequence[MigrationResource]) -> None:
        self._resources = self.validated_resources(resources)

    def validated_resources(
        self, resources: Sequence[MigrationResource]
    ) -> Sequence[MigrationResource]:
        return resources


class ExecuteAllAction(MigrationAction):
    pass


class ExecuteNonBlockingAction(MigrationAction):
    pass


class ExecuteNoneAction(MigrationAction):
    pass


MIGRATIONS_RESOURCES = {
    group: MigrationResource(group) for group in settings.ADMIN_ALLOWED_MIGRATION_GROUPS
}


@dataclass(frozen=True)
class Role:
    name: str
    actions: Set[MigrationAction]


def generate_test_role(
    group: str,
    policy: str,
    override_resource: bool = False,
    name: Optional[str] = None,
) -> Role:
    if not name:
        name = f"{group}-{policy}"

    if policy == "all":
        action: Type[MigrationAction] = ExecuteAllAction
    elif policy == "non_blocking":
        action = ExecuteNonBlockingAction
    else:
        action = ExecuteNoneAction

    resource = (
        MigrationResource(group) if override_resource else MIGRATIONS_RESOURCES[group]
    )

    return Role(name=name, actions={action([resource])})


ROLES = {
    "MigrationsReader": Role(
        name="MigrationsReader",
        actions={ExecuteNoneAction(list(MIGRATIONS_RESOURCES.values()))},
    ),
    "NonBlockingMigrationsExecutor": Role(
        name="NonBlockingMigrationsExecutor",
        actions={ExecuteNonBlockingAction(list(MIGRATIONS_RESOURCES.values()))},
    ),
    "TestMigrationsExecutor": Role(
        name="TestMigrationsExecutor",
        actions={ExecuteAllAction([MIGRATIONS_RESOURCES["test_migration"]])},
    ),
    "SearchIssuesExecutor": Role(
        name="SearchIssuesExecutor",
        actions={ExecuteNonBlockingAction([MIGRATIONS_RESOURCES["search_issues"]])},
    ),
}

DEFAULT_ROLES = [
    ROLES["MigrationsReader"],
    ROLES["TestMigrationsExecutor"],
    ROLES["SearchIssuesExecutor"],
]
