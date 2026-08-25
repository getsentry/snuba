import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import (
    PassthroughPolicy,
    get_active_allocation_policies,
)
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from tests.query.allocation_policies.attachment import (
    CURRENT_EAP_ATTACHMENT,
    match_block,
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
        "ConcurrentRateLimitAllocationPolicy",
    ]
    assert policies[0]._resource_identifier.value == "EAP"
    assert policies[0].get_config_value("concurrent_limit") == 66
    assert policies[0].is_enforced is False


def test_unknown_policy_is_skipped() -> None:
    with override_allocation_policy(
        {
            "EAP": match_block(
                [
                    {"name": "DoesNotExist"},
                    {"name": "ReferrerGuardRailPolicy"},
                ]
            )
        }
    ):
        policies = get_active_allocation_policies(ResourceIdentifier("EAP"))
    assert [p.class_name() for p in policies] == ["ReferrerGuardRailPolicy"]


@pytest.mark.parametrize(
    "spec",
    _POLICY_SPECS,
    ids=lambda spec: str(spec["name"]),
)
def test_each_policy_constructs(spec: dict[str, object]) -> None:
    with override_allocation_policy({"errors": match_block([spec])}):
        policies = get_active_allocation_policies(ResourceIdentifier("errors"))

    assert [p.class_name() for p in policies] == [spec["name"]]

    constructed = policies[0]
    assert constructed._resource_identifier.value == "errors"
    assert constructed.is_enforced is False


def test_all_unknown_falls_back_to_passthrough() -> None:
    with override_allocation_policy({"EAP": match_block([{"name": "DoesNotExist"}])}):
        policies = get_active_allocation_policies(ResourceIdentifier("EAP"))
    assert len(policies) == 1
    assert isinstance(policies[0], PassthroughPolicy)


def test_matching_org_block_adds_policy() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "is_enforced": 0,
                        }
                    ],
                },
                {
                    "match": {"organization_id": [1]},
                    "policies": [
                        {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
                    ],
                },
            ]
        }
    ):
        for_org_1 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 1}
        )
        for_org_2 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 2}
        )

    assert [p.class_name() for p in for_org_1] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    ]
    assert [p.class_name() for p in for_org_2] == ["ConcurrentRateLimitAllocationPolicy"]


def test_later_matching_block_replaces_policy_by_name() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "concurrent_limit": 66,
                            "is_enforced": 0,
                        }
                    ],
                },
                {
                    "match": {"organization_id": [1]},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "concurrent_limit": 10,
                            "is_enforced": 1,
                        }
                    ],
                },
            ]
        }
    ):
        for_org_1 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 1}
        )
        for_org_2 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 2}
        )

    assert [p.class_name() for p in for_org_1] == ["ConcurrentRateLimitAllocationPolicy"]
    assert for_org_1[0].get_config_value("concurrent_limit") == 10
    assert for_org_1[0].is_enforced is True
    assert [p.class_name() for p in for_org_2] == ["ConcurrentRateLimitAllocationPolicy"]
    assert for_org_2[0].get_config_value("concurrent_limit") == 66
    assert for_org_2[0].is_enforced is False


def test_matching_block_can_remove_a_policy() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "is_enforced": 0,
                        },
                        {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
                    ],
                },
                {
                    "match": {"organization_id": [1]},
                    "remove": ["ReferrerGuardRailPolicy"],
                },
            ]
        }
    ):
        for_org_1 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 1}
        )
        for_org_2 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 2}
        )

    assert [p.class_name() for p in for_org_1] == ["ConcurrentRateLimitAllocationPolicy"]
    assert [p.class_name() for p in for_org_2] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    ]


def test_match_list_is_or_within_key() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "is_enforced": 0,
                        }
                    ],
                },
                {
                    "match": {"organization_id": [1, 2]},
                    "policies": [
                        {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
                    ],
                },
            ]
        }
    ):
        for_org_1 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 1}
        )
        for_org_2 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 2}
        )
        for_org_3 = get_active_allocation_policies(
            ResourceIdentifier("EAP"), {"organization_id": 3}
        )

    assert [p.class_name() for p in for_org_1] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    ]
    assert [p.class_name() for p in for_org_2] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    ]
    assert [p.class_name() for p in for_org_3] == ["ConcurrentRateLimitAllocationPolicy"]


def test_match_requires_every_key() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "is_enforced": 0,
                        }
                    ],
                },
                {
                    "match": {
                        "organization_id": [1],
                        "referrer": ["api.search"],
                    },
                    "policies": [
                        {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
                    ],
                },
            ]
        }
    ):
        both = get_active_allocation_policies(
            ResourceIdentifier("EAP"),
            {"organization_id": 1, "referrer": "api.search"},
        )
        org_only = get_active_allocation_policies(ResourceIdentifier("EAP"), {"organization_id": 1})
        other_referrer = get_active_allocation_policies(
            ResourceIdentifier("EAP"),
            {"organization_id": 1, "referrer": "api.other"},
        )

    names = ["ConcurrentRateLimitAllocationPolicy", "ReferrerGuardRailPolicy"]
    assert [p.class_name() for p in both] == names
    assert [p.class_name() for p in org_only] == ["ConcurrentRateLimitAllocationPolicy"]
    assert [p.class_name() for p in other_referrer] == ["ConcurrentRateLimitAllocationPolicy"]


def test_eap_routing_strategy_uses_tenant_ids() -> None:
    with override_allocation_policy(
        {
            "EAP": [
                {
                    "match": {},
                    "policies": [
                        {
                            "name": "ConcurrentRateLimitAllocationPolicy",
                            "is_enforced": 0,
                        }
                    ],
                },
                {
                    "match": {"organization_id": [1]},
                    "policies": [
                        {"name": "ReferrerGuardRailPolicy", "is_enforced": 0},
                    ],
                },
            ]
        }
    ):
        strategy = OutcomesBasedRoutingStrategy()
        for_org_1 = strategy.get_allocation_policies({"organization_id": 1})
        for_org_2 = strategy.get_allocation_policies({"organization_id": 2})

    assert [p.class_name() for p in for_org_1] == [
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    ]
    assert [p.class_name() for p in for_org_2] == ["ConcurrentRateLimitAllocationPolicy"]
