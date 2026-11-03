"""The `key_by` setting shared by the policies that keep Redis counts per tenant.

`key_by` picks which tenant a query counts against. Each policy accepts the values
it supports, defaults to the one that keeps its existing Redis keys, and builds
its own key from the `CountedTenant` returned here.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass

from snuba.configs.configuration import Configuration
from snuba.query.allocation_policies import (
    AllocationPolicy,
    InvalidTenantsForAllocationPolicy,
)

logger = logging.getLogger(__name__)

KEY_BY = "key_by"

# project_id if present, else organization_id
TENANT = "tenant"
TENANT_REFERRER = "tenant_referrer"
ORGANIZATION = "organization"
ORGANIZATION_REFERRER = "organization_referrer"

_BY_ORGANIZATION = {ORGANIZATION, ORGANIZATION_REFERRER}
_WITH_REFERRER = {TENANT_REFERRER, ORGANIZATION_REFERRER}


@dataclass(frozen=True)
class CountedTenant:
    key_by: str
    key: str
    value: str | int
    # Set only when the count is kept per referrer too.
    referrer: str | None

    def window_prefix(self, component_name: str) -> str:
        prefix = f"{component_name}-{self.key}-{self.value}"
        return prefix if self.referrer is None else f"{prefix}-{self.referrer}"


def key_by_config(default: str, choices: Collection[str]) -> Configuration:
    return Configuration(
        name=KEY_BY,
        description=(
            f"Which tenant a query counts against: one of {', '.join(sorted(choices))}. "
            f"{TENANT} is project_id if present, else organization_id. The *_referrer "
            f"values also count per referrer. Per-tenant limits go in match blocks. "
            f"Queries without an organization_id use {default}"
        ),
        value_type=str,
        default=default,
    )


def tenant_key_and_value(
    tenant_ids: Mapping[str, str | int], policy_name: str
) -> tuple[str, str | int]:
    if "project_id" in tenant_ids:
        return "project_id", tenant_ids["project_id"]
    if "organization_id" in tenant_ids:
        return "organization_id", tenant_ids["organization_id"]
    raise InvalidTenantsForAllocationPolicy.from_args(
        dict(tenant_ids),
        policy_name,
        "tenant_ids must include organization_id or project id",
    )


def get_counted_tenant(
    policy: AllocationPolicy,
    tenant_ids: Mapping[str, str | int],
    default: str,
    choices: Collection[str],
) -> CountedTenant:
    key_by = policy.get_config_value(KEY_BY)
    if key_by not in choices:
        logger.warning("Unknown key_by %s on %s", key_by, policy.component_name())
        key_by = default
    if key_by in _BY_ORGANIZATION and "organization_id" not in tenant_ids:
        key_by = default

    if key_by in _BY_ORGANIZATION and "organization_id" in tenant_ids:
        key, value = "organization_id", tenant_ids["organization_id"]
    else:
        key, value = tenant_key_and_value(tenant_ids, policy.class_name())
    referrer = str(tenant_ids.get("referrer", "no_referrer")) if key_by in _WITH_REFERRER else None
    return CountedTenant(key_by, key, value, referrer)
