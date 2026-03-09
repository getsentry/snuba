from typing import Sequence, Set

import pytest

from snuba.admin.auth_roles import (
    ExecuteAllAction,
    ExecuteNonBlockingAction,
    ExecuteNoneAction,
    MigrationResource,
    Role,
)
from snuba.admin.migrations_policies import get_migration_group_policies
from snuba.admin.user import AdminUser
from snuba.migrations.policies import (
    AllMigrationsPolicy,
    MigrationPolicy,
    NoMigrationsPolicy,
    NonBlockingMigrationsPolicy,
)

NO_MIGRATIONS_ROLE = Role(
    "none",
    actions={ExecuteNoneAction([MigrationResource("test_migration")])},
)
NON_BLOCKING_ROLE = Role(
    "non blocking",
    actions={ExecuteNonBlockingAction([MigrationResource("test_migration")])},
)
ALL_ROLE = Role("all", actions={ExecuteAllAction([MigrationResource("test_migration")])})


@pytest.mark.parametrize(
    "roles, expected_policies",
    [
        (
            [ALL_ROLE],
            {AllMigrationsPolicy()},
        ),
        (
            [NO_MIGRATIONS_ROLE, NON_BLOCKING_ROLE],
            {NoMigrationsPolicy(), NonBlockingMigrationsPolicy()},
        ),
    ],
)
def test_get_group_policies(roles: Sequence[Role], expected_policies: Set[MigrationPolicy]) -> None:
    user = AdminUser("meredith@sentry.io", "123", roles=roles)
    results = get_migration_group_policies(user)
    assert set(r.__class__ for r in results["test_migration"]) == set(
        e.__class__ for e in expected_policies
    )


def test_get_migration_group_policies_sans_roles() -> None:
    user = AdminUser("meredith@sentry.io", "123", roles=[])
    results = get_migration_group_policies(user)
    assert results == {}
