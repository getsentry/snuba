from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sentry_options.testing import override_options

from snuba.configs.configuration import CONFIGURABLE_COMPONENT_OVERRIDES_KEY
from snuba.query.allocation_policies import ALLOCATION_POLICY_ATTACHMENT_KEY

# Today's live EAP attachment (was hardcoded on StorageRouting).
CURRENT_EAP_ATTACHMENT: list[dict[str, Any]] = [
    {
        "name": "ConcurrentRateLimitAllocationPolicy",
        "required_tenant_types": ["organization_id", "referrer", "project_id"],
    },
    {
        "name": "ReferrerGuardRailPolicy",
        "required_tenant_types": ["referrer"],
    },
    {
        "name": "BytesScannedRejectingPolicy",
        "required_tenant_types": ["organization_id", "project_id", "referrer"],
    },
]

# Ctor default_config_overrides that used to live on StorageRouting.get_allocation_policies.
CURRENT_EAP_SETTINGS: dict[str, int] = {
    "EAP.ConcurrentRateLimitAllocationPolicy.is_enforced": 0,
    "EAP.ConcurrentRateLimitAllocationPolicy.concurrent_limit": 66,
    "EAP.ReferrerGuardRailPolicy.is_enforced": 0,
    "EAP.ReferrerGuardRailPolicy.is_active": 0,
    "EAP.BytesScannedRejectingPolicy.is_active": 0,
    "EAP.BytesScannedRejectingPolicy.is_enforced": 0,
}


@contextmanager
def override_allocation_policy_attachment(
    attachment: dict[str, list[dict[str, Any]]],
) -> Iterator[None]:
    with override_options("snuba", {ALLOCATION_POLICY_ATTACHMENT_KEY: attachment}):
        yield


@contextmanager
def override_current_eap_policies() -> Iterator[None]:
    """Attach today's EAP policy list and the settings the hardcoded ctor used to inject."""
    with override_options(
        "snuba",
        {
            ALLOCATION_POLICY_ATTACHMENT_KEY: {"EAP": CURRENT_EAP_ATTACHMENT},
            CONFIGURABLE_COMPONENT_OVERRIDES_KEY: CURRENT_EAP_SETTINGS,
        },
    ):
        yield
