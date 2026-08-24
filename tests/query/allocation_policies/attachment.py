from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sentry_options.testing import override_options

from snuba.query.allocation_policies import ALLOCATION_POLICY_KEY

# Today's live EAP attachment (was hardcoded on StorageRouting).
CURRENT_EAP_ATTACHMENT: list[dict[str, Any]] = [
    {
        "name": "ConcurrentRateLimitAllocationPolicy",
        "required_tenant_types": ["organization_id", "referrer", "project_id"],
        "concurrent_limit": 66,
        "is_enforced": 0,
    },
    {
        "name": "ReferrerGuardRailPolicy",
        "required_tenant_types": ["referrer"],
        "is_active": 0,
        "is_enforced": 0,
    },
    {
        "name": "BytesScannedRejectingPolicy",
        "required_tenant_types": ["organization_id", "project_id", "referrer"],
        "is_active": 0,
        "is_enforced": 0,
    },
]


@contextmanager
def override_allocation_policy(
    attachment: dict[str, list[dict[str, Any]]],
) -> Iterator[None]:
    with override_options("snuba", {ALLOCATION_POLICY_KEY: attachment}):
        yield


@contextmanager
def override_current_eap_policies() -> Iterator[None]:
    """Attach today's EAP policy list and the settings the hardcoded ctor used to inject."""
    with override_allocation_policy({"EAP": CURRENT_EAP_ATTACHMENT}):
        yield
