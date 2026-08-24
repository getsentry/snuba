import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import (
    PassthroughPolicy,
    get_active_allocation_policies,
)
from tests.query.allocation_policies.attachment import (
    CURRENT_EAP_ATTACHMENT,
    override_allocation_policy,
)

_POLICY_SPECS: list[dict[str, object]] = [
    {"name": "BytesScannedRejectingPolicy", "is_enforced": 0},
    {"name": "BytesScannedWindowAllocationPolicy", "is_enforced": 0},
    {"name": "ConcurrentRateLimitAllocationPolicy", "is_enforced": 0},
    {
        "name": "CrossOrgQueryAllocationPolicy",
        "is_enforced": 0,
        "cross_org_referrer_limits": {"some.referrer": {"max_threads": 2, "concurrent_limit": 4}},
    },
    {"name": "DeleteConcurrentRateLimitAllocationPolicy", "is_enforced": 0},
    {"name": "PassthroughPolicy", "is_enforced": 0},
    {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
]


def test_unset_falls_back_to_passthrough() -> None:
    policies = get_active_allocation_policies(ResourceIdentifier("errors"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)
    assert policies[0]._resource_identifier.value == "errors"


def test_empty_list_falls_back_to_passthrough() -> None:
    with override_allocation_policy({"errors": []}):
        policies = get_active_allocation_policies(ResourceIdentifier("errors"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)


def test_eap_attachment_constructs_named_policies() -> None:
    with override_allocation_policy({"EAP": CURRENT_EAP_ATTACHMENT}):
        policies = get_active_allocation_policies(ResourceIdentifier("EAP"))

    assert [p.class_name() for p in policies] == [
        "PassthroughPolicy",
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
        "BytesScannedRejectingPolicy",
    ]
    assert policies[1]._resource_identifier.value == "EAP"
    assert policies[1]._required_tenant_types == {
        "organization_id",
        "project_id",
        "referrer",
    }
    assert policies[1].get_config_value("concurrent_limit") == 66
    assert policies[1].is_enforced is False
    assert policies[2].is_active is False


def test_unknown_policy_is_skipped() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {"name": "DoesNotExist"},
                {"name": "ReferrerGuardRailPolicy"},
            ]
        }
    ):
        policies = get_active_allocation_policies(ResourceIdentifier("EAP"))
    assert [p.class_name() for p in policies] == ["PassthroughPolicy", "ReferrerGuardRailPolicy"]


@pytest.mark.parametrize(
    "spec",
    _POLICY_SPECS,
    ids=lambda spec: str(spec["name"]),
)
def test_each_policy_constructs(spec: dict[str, object]) -> None:
    with override_allocation_policy({"errors": [spec]}):
        policies = get_active_allocation_policies(ResourceIdentifier("errors"))

    assert [p.class_name() for p in policies] == ["PassthroughPolicy", spec["name"]]

    constructed = policies[1]
    assert constructed._resource_identifier.value == "errors"
    assert constructed._required_tenant_types == set(type(constructed).required_tenant_types)
    assert constructed.is_enforced is False


def test_all_unknown_falls_back_to_passthrough() -> None:
    with override_allocation_policy({"EAP": [{"name": "DoesNotExist"}]}):
        policies = get_active_allocation_policies(ResourceIdentifier("EAP"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)
