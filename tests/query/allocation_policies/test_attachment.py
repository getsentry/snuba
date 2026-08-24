from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import (
    PassthroughPolicy,
    get_attached_allocation_policies,
)
from tests.query.allocation_policies.attachment import (
    CURRENT_EAP_ATTACHMENT,
    override_allocation_policy_attachment,
)


def test_unset_falls_back_to_passthrough() -> None:
    policies = get_attached_allocation_policies(ResourceIdentifier("errors"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)
    assert policies[0]._resource_identifier.value == "errors"


def test_empty_list_falls_back_to_passthrough() -> None:
    with override_allocation_policy_attachment({"errors": []}):
        policies = get_attached_allocation_policies(ResourceIdentifier("errors"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)


def test_eap_attachment_constructs_named_policies() -> None:
    with override_allocation_policy_attachment({"EAP": CURRENT_EAP_ATTACHMENT}):
        policies = get_attached_allocation_policies(ResourceIdentifier("EAP"))
    assert [p.class_name() for p in policies] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
        "BytesScannedRejectingPolicy",
    ]
    assert policies[0]._resource_identifier.value == "EAP"
    assert policies[0]._required_tenant_types == {
        "organization_id",
        "referrer",
        "project_id",
    }


def test_unknown_policy_is_skipped() -> None:
    with override_allocation_policy_attachment(
        {
            "EAP": [
                {"name": "DoesNotExist", "required_tenant_types": ["referrer"]},
                {
                    "name": "ReferrerGuardRailPolicy",
                    "required_tenant_types": ["referrer"],
                },
            ]
        }
    ):
        policies = get_attached_allocation_policies(ResourceIdentifier("EAP"))
    assert [p.class_name() for p in policies] == ["ReferrerGuardRailPolicy"]


def test_all_unknown_falls_back_to_passthrough() -> None:
    with override_allocation_policy_attachment(
        {"EAP": [{"name": "DoesNotExist", "required_tenant_types": []}]}
    ):
        policies = get_attached_allocation_policies(ResourceIdentifier("EAP"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)
