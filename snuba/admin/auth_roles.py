from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Set

from snuba import settings


class Category(Enum):
    MIGRATIONS = "migrations"


@dataclass(frozen=True)
class Resource:
    name: str
    category: Category


class Action:
    def __init__(self, resources: Sequence[Resource]) -> None:
        self._resources = self.validated_resources(resources)

    def validated_resources(self, resources: Sequence[Resource]) -> Sequence[Resource]:
        raise NotImplementedError


class MigrationAction(Action):
    def validated_resources(self, resources: Sequence[Resource]) -> Sequence[Resource]:
        invalid_resources = [
            resource.name
            for resource in resources
            if resource.category != Category.MIGRATIONS
        ]
        if invalid_resources:
            raise Exception(
                f"These resources aren't cateogry Migrations {invalid_resources}"
            )
        return resources


class ExecuteAllAction(MigrationAction):
    pass


class ExecuteNonBlockingAction(MigrationAction):
    pass


class ExecuteNoneAction(MigrationAction):
    pass


# class MigrationAction:
#     EXECUTE_ALL = "execute_all"
#     EXECUTE_NONBLOCKING = "execute_nonblocking"
#     EXECUTE_NONE = "execute_none"


# todo, shoudln't need .keys() once ADMIN_ALLOWED_MIGRATION_GROUPS is a set not dict
MIGRATIONS_RESOURCES = {
    group: Resource(group, Category.MIGRATIONS)
    for group in settings.ADMIN_ALLOWED_MIGRATION_GROUPS.keys()
}


@dataclass(frozen=True)
class Role:
    name: str
    actions: Set[Action]


DEFAULT_ROLES = [
    Role(
        name="MigrationsReader",
        actions={ExecuteNoneAction(resources=list(MIGRATIONS_RESOURCES.values()))},
    ),
    Role(
        name="MigrationsLimitedExecutor",
        actions={
            ExecuteNonBlockingAction(resources=list(MIGRATIONS_RESOURCES.values()))
        },
    ),
    Role(
        name="TestMigrationsExecutor",
        actions={ExecuteAllAction(resources=[MIGRATIONS_RESOURCES["test_migration"]])},
    ),
]

# DEFAULT_ROLES = [
#     Role(
#         name="MigrationsReader",
#         resources=MIGRATIONS_RESOURCES,
#         actions={Action.READ},
#     ),
#     Role(
#         name="MigrationsLimitedExecutor",
#         resources=MIGRATIONS_RESOURCES,
#         actions={Action.LIMITED_EXECUTE},
#     ),
#     Role(
#         name="TestMigrationsExecutor",
#         resources={"migrations.test_migration"},
#         actions={Action.EXECUTE},
#     ),
# ]
