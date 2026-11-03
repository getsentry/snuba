import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import InvalidTenantsForAllocationPolicy
from snuba.query.allocation_policies.concurrent_rate_limit import (
    ConcurrentRateLimitAllocationPolicy,
)
from snuba.query.allocation_policies.key_by import (
    ORGANIZATION,
    ORGANIZATION_REFERRER,
    TENANT,
    TENANT_REFERRER,
    CountedTenant,
    get_counted_tenant,
    tenant_key_and_value,
)

_CHOICES = {TENANT, TENANT_REFERRER, ORGANIZATION, ORGANIZATION_REFERRER}


def test_tenant_key_and_value() -> None:
    assert tenant_key_and_value({"organization_id": 1, "project_id": 2}, "P") == (
        "project_id",
        2,
    )
    assert tenant_key_and_value({"organization_id": 1}, "P") == ("organization_id", 1)
    with pytest.raises(InvalidTenantsForAllocationPolicy):
        tenant_key_and_value({}, "P")


@pytest.mark.parametrize(
    ("key_by", "tenant_ids", "expected"),
    [
        pytest.param(
            TENANT,
            {"organization_id": 1, "project_id": 2, "referrer": "r"},
            CountedTenant(TENANT, "project_id", 2, None),
            id="tenant",
        ),
        pytest.param(
            TENANT_REFERRER,
            {"organization_id": 1, "referrer": "r"},
            CountedTenant(TENANT_REFERRER, "organization_id", 1, "r"),
            id="tenant_referrer",
        ),
        pytest.param(
            ORGANIZATION,
            {"organization_id": 1, "project_id": 2, "referrer": "r"},
            CountedTenant(ORGANIZATION, "organization_id", 1, None),
            id="organization",
        ),
        pytest.param(
            ORGANIZATION_REFERRER,
            {"organization_id": 1, "project_id": 2},
            CountedTenant(ORGANIZATION_REFERRER, "organization_id", 1, "no_referrer"),
            id="organization_referrer",
        ),
        pytest.param(
            ORGANIZATION_REFERRER,
            {"project_id": 2, "referrer": "r"},
            CountedTenant(TENANT, "project_id", 2, None),
            id="no organization_id uses the default",
        ),
        pytest.param(
            "bogus",
            {"organization_id": 1, "referrer": "r"},
            CountedTenant(TENANT, "organization_id", 1, None),
            id="unknown value uses the default",
        ),
    ],
)
def test_get_counted_tenant(
    key_by: str, tenant_ids: dict[str, str | int], expected: CountedTenant
) -> None:
    policy = ConcurrentRateLimitAllocationPolicy(
        ResourceIdentifier(StorageKey("errors")), key_by=key_by
    )
    assert get_counted_tenant(policy, tenant_ids, TENANT, _CHOICES) == expected


def test_value_outside_choices_uses_the_default() -> None:
    policy = ConcurrentRateLimitAllocationPolicy(
        ResourceIdentifier(StorageKey("errors")), key_by=TENANT_REFERRER
    )
    counted = get_counted_tenant(policy, {"organization_id": 1}, ORGANIZATION, {ORGANIZATION})
    assert counted == CountedTenant(ORGANIZATION, "organization_id", 1, None)


def test_window_prefix() -> None:
    assert CountedTenant(ORGANIZATION, "organization_id", 1, None).window_prefix("c") == (
        "c-organization_id-1"
    )
    assert CountedTenant(TENANT_REFERRER, "project_id", 2, "r").window_prefix("c") == (
        "c-project_id-2-r"
    )
