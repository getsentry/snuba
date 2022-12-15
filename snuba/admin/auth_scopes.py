from dataclasses import dataclass
from enum import Enum
from typing import Set


class AdminRole(Enum):
    ADMIN = "admin"
    MEMBER = "member"
    MEMBER_READ = "member_read"


ADMIN_CATEGORY_RESOURCES = {
    # clickhouse migrations
    "migrations.all",
    "migrations.system",
    "migrations.generic_metrics",
    "migrations.profiles",
    "migrations.functions",
    "migrations.replays",
    "migrations.querylog",
    "migrations.test_migration",
}


@dataclass(frozen=True)
class AuthScope:
    category: str
    resource: str
    role: AdminRole

    def to_str(self) -> str:
        return f"{self.category}.{self.resource}.{self.role.value}"


def scopes() -> Set[AuthScope]:
    scopes = set()
    for item in ADMIN_CATEGORY_RESOURCES:
        category, resource = item.split(".")
        scopes.update([AuthScope(category, resource, role) for role in AdminRole])
    return scopes


ADMIN_SCOPES = scopes()
