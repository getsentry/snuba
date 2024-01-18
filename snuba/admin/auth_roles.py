from __future__ import annotations

from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Sequence, Set, TypeVar

from snuba import settings
from snuba.migrations.runner import get_active_migration_groups


class Category(Enum):
    MIGRATIONS = "migrations"
    TOOLS = "tools"


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


class ToolResource(Resource):
    @property
    def category(self) -> Category:
        return Category.TOOLS


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


class ToolAction(Action[ToolResource]):
    def __init__(self, resources: Sequence[ToolResource]) -> None:
        self._resources = self.validated_resources(resources)

    def validated_resources(
        self, resources: Sequence[ToolResource]
    ) -> Sequence[ToolResource]:
        return resources


class InteractToolAction(ToolAction):
    # Gives users access to view a tool on the admin page
    pass


TOOL_RESOURCES = {
    "snql-to-sql": ToolResource("snql-to-sql"),
    "tracing": ToolResource("tracing"),
    "cardinality-analyzer": ToolResource("cardinality-analyzer"),
    "production-queries": ToolResource("production-queries"),
    "system-queries": ToolResource("system-queries"),
    "all": ToolResource("all"),
}


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
    **{
        group.value: MigrationResource(group.value)
        for group in get_active_migration_groups()
    },
    **{group: MigrationResource(group) for group in settings.SKIPPED_MIGRATION_GROUPS},
}


@dataclass(frozen=True)
class Role:
    name: str
    actions: Set[MigrationAction | ToolAction]


def generate_tool_test_role(tool: str) -> Role:
    return Role(name=tool, actions={InteractToolAction([ToolResource(tool)])})


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
    "AllTools": Role(
        name="all-tools",
        actions={InteractToolAction([TOOL_RESOURCES["all"]])},
    ),
    "ProductTools": Role(
        name="product-tools",
        actions={
            InteractToolAction(
                [
                    TOOL_RESOURCES["snql-to-sql"],
                    TOOL_RESOURCES["tracing"],
                    TOOL_RESOURCES["production-queries"],
                    TOOL_RESOURCES["system-queries"],
                ]
            )
        },
    ),
    "CardinalityAnalyzer": Role(
        name="cardinality-analyzer",
        actions={InteractToolAction([TOOL_RESOURCES["cardinality-analyzer"]])},
    ),
    "AllMigrationsExecutor": Role(
        name="AllMigrationsExecutor",
        actions={ExecuteAllAction(list(MIGRATIONS_RESOURCES.values()))},
    ),
}

DEFAULT_ROLES = [
    ROLES["MigrationsReader"],
    ROLES["TestMigrationsExecutor"],
    ROLES["SearchIssuesExecutor"],
    ROLES["ProductTools"],
]

if settings.TESTING or settings.DEBUG:
    DEFAULT_ROLES.append(ROLES["AllTools"])
