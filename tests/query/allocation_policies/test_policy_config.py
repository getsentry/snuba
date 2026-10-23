import copy
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from sentry_options.testing import override_options

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import AllocationPolicy, PassthroughPolicy
from snuba.query.allocation_policies.policy_config import BASE_KEY, REGION_KEY
from snuba.query.allocation_policies.resolver import (
    get_active_allocation_policies,
    get_active_delete_allocation_policies,
)
from tests.query.allocation_policies.attachment import override_allocation_policy


@contextmanager
def override_policy_config(
    base: dict[str, Any] | None = None,
    region: dict[str, Any] | None = None,
) -> Iterator[None]:
    with override_options("snuba", {BASE_KEY: base or {}, REGION_KEY: region or {}}):
        yield


def _snapshot(policies: list[AllocationPolicy]) -> dict[str, dict[str, Any]]:
    """Class name -> every unparameterized config value, plus thresholds."""
    return {
        policy.class_name(): {
            **{
                name: policy.get_config_value(name)
                for name, definition in policy.config_definitions().items()
                if not definition.param_types
            },
            "thresholds": policy.thresholds,
        }
        for policy in policies
    }


# us/values.yaml `transactions` as of 2026-10-23, in the legacy format.
_LEGACY_TRANSACTIONS = [
    {
        "match": {},
        "policies": [
            {
                "name": "BytesScannedWindowAllocationPolicy",
                "org_limit_bytes_scanned": 400000000000,
                "throttled_thread_number": 4,
            },
            {
                "name": "ConcurrentRateLimitAllocationPolicy",
                "is_enforced": 1,
                "concurrent_limit": 20,
            },
            {
                "name": "ReferrerGuardRailPolicy",
                "is_enforced": 1,
                "default_concurrent_request_per_referrer": 40,
            },
        ],
    },
    {
        "match": {"referrer": ["subscriptions_executor"]},
        "policies": [
            {
                "name": "ReferrerGuardRailPolicy",
                "is_enforced": 1,
                "default_concurrent_request_per_referrer": 100,
            }
        ],
    },
    {
        "match": {"project_id": [5868616]},
        "policies": [
            {
                "name": "BytesScannedRejectingPolicy",
                "is_enforced": 1,
                "project_referrer_scan_limit": 200000000000,
            }
        ],
    },
]

# The same config in the new format.
_NEW_TRANSACTIONS = {
    "resources": {
        "transactions": {
            "enforce": True,
            "policies": {
                "bytes_scanned_window": {
                    "org_bytes_limit": 400000000000,
                    "throttled_max_threads": 4,
                },
                "concurrent_rate_limit": {"concurrent_limit": 20},
                "referrer_guard_rail": {"concurrent_limit": 40},
            },
            "overrides": [
                {
                    "match": {"referrer": ["subscriptions_executor"]},
                    "policies": {"referrer_guard_rail": {"concurrent_limit": 100}},
                },
                {
                    "match": {"project_id": [5868616]},
                    "policies": {"bytes_scanned_rejecting": {"project_bytes_limit": 200000000000}},
                },
            ],
        }
    }
}


@pytest.mark.parametrize(
    "tenant_ids",
    [
        {},
        {"referrer": "subscriptions_executor"},
        {"project_id": 5868616},
    ],
    ids=["no-tenant", "referrer-override", "project-override"],
)
def test_new_format_matches_legacy_transactions(tenant_ids: dict[str, str | int]) -> None:
    resource = ResourceIdentifier("transactions")
    with override_allocation_policy({"transactions": _LEGACY_TRANSACTIONS}):
        legacy = _snapshot(get_active_allocation_policies(resource, tenant_ids))
    with override_policy_config(base=_NEW_TRANSACTIONS):
        new = _snapshot(get_active_allocation_policies(resource, tenant_ids))

    assert new == legacy


def test_defaults_and_resource_settings_are_inherited() -> None:
    config = {
        "defaults": {"enforce": False, "max_threads": 16},
        "resources": {
            "errors": {
                "max_threads": 8,
                "policies": {
                    "concurrent_rate_limit": {},
                    "referrer_guard_rail": {"enforce": True, "max_threads": 4},
                },
            }
        },
    }
    with override_policy_config(base=config):
        policies = _snapshot(get_active_allocation_policies(ResourceIdentifier("errors")))

    assert policies["ConcurrentRateLimitAllocationPolicy"]["is_enforced"] == 0
    assert policies["ConcurrentRateLimitAllocationPolicy"]["max_threads"] == 8
    assert policies["ReferrerGuardRailPolicy"]["is_enforced"] == 1
    assert policies["ReferrerGuardRailPolicy"]["max_threads"] == 4


def test_override_patches_one_field_and_keeps_the_rest() -> None:
    config = {
        "resources": {
            "replays": {
                "enforce": False,
                "max_threads": 8,
                "policies": {"concurrent_rate_limit": {"concurrent_limit": 22}},
                "overrides": [
                    {
                        "match": {"organization_id": [304281]},
                        "policies": {"concurrent_rate_limit": {"concurrent_limit": 32}},
                    }
                ],
            }
        }
    }
    with override_policy_config(base=config):
        policy = _snapshot(
            get_active_allocation_policies(
                ResourceIdentifier("replays"), {"organization_id": 304281}
            )
        )["ConcurrentRateLimitAllocationPolicy"]

    assert policy["concurrent_limit"] == 32
    assert policy["is_enforced"] == 0
    assert policy["max_threads"] == 8


@pytest.mark.parametrize("reverse", [False, True], ids=["specific-last", "specific-first"])
def test_more_specific_override_wins_regardless_of_order(reverse: bool) -> None:
    overrides = [
        {
            "match": {"referrer": ["api.foo"]},
            "policies": {"concurrent_rate_limit": {"concurrent_limit": 1}},
        },
        {
            "match": {"organization_id": [1]},
            "policies": {"concurrent_rate_limit": {"concurrent_limit": 2}},
        },
        {
            "match": {"organization_id": [1], "referrer": ["api.foo"]},
            "policies": {"concurrent_rate_limit": {"concurrent_limit": 3}},
        },
    ]
    if reverse:
        overrides.reverse()
    config = {
        "resources": {
            "errors": {
                "policies": {"concurrent_rate_limit": {"concurrent_limit": 50}},
                "overrides": overrides,
            }
        }
    }

    def limit(tenant_ids: dict[str, str | int]) -> int:
        policies = get_active_allocation_policies(ResourceIdentifier("errors"), tenant_ids)
        return int(_snapshot(policies)["ConcurrentRateLimitAllocationPolicy"]["concurrent_limit"])

    with override_policy_config(base=config):
        assert limit({}) == 50
        assert limit({"referrer": "api.foo"}) == 1
        assert limit({"organization_id": 1}) == 2
        assert limit({"organization_id": 1, "referrer": "api.foo"}) == 3


def test_override_can_add_and_disable_policies() -> None:
    config = {
        "resources": {
            "errors": {
                "policies": {"concurrent_rate_limit": {}, "referrer_guard_rail": {}},
                "overrides": [
                    {
                        "match": {"organization_id": [1]},
                        "policies": {
                            "referrer_guard_rail": {"enabled": False},
                            "bytes_scanned_rejecting": {},
                        },
                    }
                ],
            }
        }
    }
    with override_policy_config(base=config):
        for_org_1 = get_active_allocation_policies(
            ResourceIdentifier("errors"), {"organization_id": 1}
        )
        for_org_2 = get_active_allocation_policies(
            ResourceIdentifier("errors"), {"organization_id": 2}
        )

    assert {p.class_name() for p in for_org_1} == {
        "ConcurrentRateLimitAllocationPolicy",
        "BytesScannedRejectingPolicy",
    }
    assert {p.class_name() for p in for_org_2} == {
        "ConcurrentRateLimitAllocationPolicy",
        "ReferrerGuardRailPolicy",
    }


def test_region_merges_over_base() -> None:
    base = {
        "defaults": {"enforce": False},
        "resources": {
            "errors": {
                "policies": {"concurrent_rate_limit": {"concurrent_limit": 22}},
                "overrides": [
                    {
                        "match": {"organization_id": [1]},
                        "policies": {"concurrent_rate_limit": {"concurrent_limit": 5}},
                    }
                ],
            }
        },
    }
    region = {
        "resources": {
            "errors": {
                "policies": {"concurrent_rate_limit": {"max_threads": 16}},
                "overrides": [
                    {
                        "match": {"organization_id": [1]},
                        "policies": {"concurrent_rate_limit": {"concurrent_limit": 7}},
                    }
                ],
            },
            "discover": {"policies": {"referrer_guard_rail": {}}},
        }
    }
    with override_policy_config(base=base, region=region):
        errors = _snapshot(get_active_allocation_policies(ResourceIdentifier("errors")))
        errors_org_1 = _snapshot(
            get_active_allocation_policies(ResourceIdentifier("errors"), {"organization_id": 1})
        )
        discover = _snapshot(get_active_allocation_policies(ResourceIdentifier("discover")))

    assert errors["ConcurrentRateLimitAllocationPolicy"]["concurrent_limit"] == 22
    assert errors["ConcurrentRateLimitAllocationPolicy"]["max_threads"] == 16
    assert errors["ConcurrentRateLimitAllocationPolicy"]["is_enforced"] == 0
    # Same specificity: the region override comes later and wins.
    assert errors_org_1["ConcurrentRateLimitAllocationPolicy"]["concurrent_limit"] == 7
    assert list(discover) == ["ReferrerGuardRailPolicy"]
    assert discover["ReferrerGuardRailPolicy"]["is_enforced"] == 0


def test_unlisted_resource_falls_back_to_legacy_option() -> None:
    legacy = {
        "errors": [
            {"match": {}, "policies": [{"name": "ReferrerGuardRailPolicy", "is_enforced": 0}]}
        ]
    }
    with (
        override_allocation_policy(legacy),
        override_policy_config(
            base={"resources": {"transactions": {"policies": {"concurrent_rate_limit": {}}}}}
        ),
    ):
        errors = get_active_allocation_policies(ResourceIdentifier("errors"))
        replays = get_active_allocation_policies(ResourceIdentifier("replays"))

    assert [p.class_name() for p in errors] == ["ReferrerGuardRailPolicy"]
    assert [type(p) for p in replays] == [PassthroughPolicy]


def test_unknown_policy_and_setting_are_dropped() -> None:
    config = {
        "resources": {
            "errors": {
                "policies": {
                    "does_not_exist": {},
                    "concurrent_rate_limit": {"concurrent_limit": 5, "typo_limit": 3},
                }
            }
        }
    }
    with override_policy_config(base=config):
        policies = _snapshot(get_active_allocation_policies(ResourceIdentifier("errors")))

    assert list(policies) == ["ConcurrentRateLimitAllocationPolicy"]
    assert policies["ConcurrentRateLimitAllocationPolicy"]["concurrent_limit"] == 5


def test_int_option_value_is_cast_for_float_config() -> None:
    config = {
        "resources": {
            "errors": {"policies": {"referrer_guard_rail": {"throttle_requests_divider": 2}}}
        }
    }
    with override_policy_config(base=config):
        policy = _snapshot(get_active_allocation_policies(ResourceIdentifier("errors")))

    assert policy["ReferrerGuardRailPolicy"]["requests_throttle_divider"] == 2.0


def test_thresholds_are_inherited() -> None:
    config = {
        "defaults": {"thresholds": {"reject": {"cluster_load": 0.5}}},
        "resources": {
            "errors": {
                "thresholds": {"reject": {"concurrent_queries": 10}},
                "policies": {"concurrent_rate_limit": {}},
            }
        },
    }
    with override_policy_config(base=config):
        (policy,) = get_active_allocation_policies(ResourceIdentifier("errors"))

    assert policy.thresholds == {"reject": {"cluster_load": 0.5, "concurrent_queries": 10.0}}


def test_option_values_are_not_mutated() -> None:
    config = copy.deepcopy(_NEW_TRANSACTIONS)
    config["defaults"] = {"thresholds": {"reject": {"cluster_load": 0.5}}}
    expected = copy.deepcopy(config)
    with override_policy_config(base=config):
        get_active_allocation_policies(ResourceIdentifier("transactions"), {"project_id": 5868616})

    assert config == expected


def test_delete_policies_only_apply_to_deletes() -> None:
    config = {
        "resources": {
            "search_issues": {
                "policies": {
                    "concurrent_rate_limit": {},
                    "delete_concurrent_rate_limit": {"concurrent_limit": 2},
                },
                "delete_policies": {
                    "delete_concurrent_rate_limit": {"concurrent_limit": 3},
                    "concurrent_rate_limit": {},
                },
            }
        }
    }
    resource = ResourceIdentifier("search_issues")
    with override_policy_config(base=config):
        reads = get_active_allocation_policies(resource)
        deletes = get_active_delete_allocation_policies(resource)

    assert [p.class_name() for p in reads] == ["ConcurrentRateLimitAllocationPolicy"]
    assert [p.class_name() for p in deletes] == ["DeleteConcurrentRateLimitAllocationPolicy"]
    assert deletes[0].get_config_value("concurrent_limit") == 3


def test_delete_policies_unset_is_empty() -> None:
    assert get_active_delete_allocation_policies(ResourceIdentifier("search_issues")) == []


def test_storage_delete_policies_come_from_option() -> None:
    config = {
        "resources": {
            "search_issues": {
                "delete_policies": {"delete_concurrent_rate_limit": {"concurrent_limit": 3}},
                "overrides": [
                    {
                        "match": {"organization_id": [1]},
                        "delete_policies": {
                            "delete_concurrent_rate_limit": {"concurrent_limit": 1}
                        },
                    }
                ],
            }
        }
    }
    storage = get_writable_storage(StorageKey.SEARCH_ISSUES)
    with override_policy_config(base=config):
        (default,) = storage.get_delete_allocation_policies()
        (for_org_1,) = storage.get_delete_allocation_policies({"organization_id": 1})

    assert default.class_name() == "DeleteConcurrentRateLimitAllocationPolicy"
    assert default.get_config_value("concurrent_limit") == 3
    assert for_org_1.get_config_value("concurrent_limit") == 1
