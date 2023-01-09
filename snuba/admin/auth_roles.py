from dataclasses import dataclass
from enum import Enum
from typing import Set

from snuba import settings


class Action(Enum):
    EXECUTE = "execute"
    LIMITED_EXECUTE = "limited_execute"
    READ = "read"


# todo, shoudln't need .keys() once ADMIN_ALLOWED_MIGRATION_GROUPS is a set not dict
MIGRATIONS_RESOURCES = set(
    [f"migrations.{group}" for group in settings.ADMIN_ALLOWED_MIGRATION_GROUPS.keys()]
)


@dataclass(frozen=True)
class Role:
    name: str
    resources: Set[str]
    actions: Set[Action]


DEFAULT_ROLES = {
    Role(
        name="MigrationsReader",
        resources=MIGRATIONS_RESOURCES,
        actions={Action.READ},
    ),
    Role(
        name="MigrationsLimitedExecutor",
        resources=MIGRATIONS_RESOURCES,
        actions={Action.LIMITED_EXECUTE},
    ),
    Role(
        name="TestMigrationsExecutor",
        resources={"migrations.test_migration"},
        actions={Action.EXECUTE},
    ),
}
