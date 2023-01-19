from typing import Sequence

import pytest

from snuba.admin.auth_roles import (
    ExecuteAllAction,
    ExecuteNonBlockingAction,
    ExecuteNoneAction,
    MigrationResource,
    Role,
)
from snuba.admin.migrations_policies import get_group_policies
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
ALL_ROLE = Role(
    "all", actions={ExecuteAllAction([MigrationResource("test_migration")])}
)


@pytest.mark.parametrize(
    "roles, expected_policy",
    [
        ([NO_MIGRATIONS_ROLE], NoMigrationsPolicy),
        ([NO_MIGRATIONS_ROLE, NON_BLOCKING_ROLE], NonBlockingMigrationsPolicy),
        ([NO_MIGRATIONS_ROLE, NON_BLOCKING_ROLE, ALL_ROLE], AllMigrationsPolicy),
    ],
)
def test_get_group_policies(
    roles: Sequence[Role], expected_policy: MigrationPolicy
) -> None:
    user = AdminUser("meredith@sentry.io", "123", roles=roles)
    results = get_group_policies(user)
    assert len(results) == 1
    assert isinstance(results["test_migration"], expected_policy)


def test_get_group_policies_sans_roles() -> None:
    user = AdminUser("meredith@sentry.io", "123", roles=[])
    results = get_group_policies(user)
    assert results == {}
