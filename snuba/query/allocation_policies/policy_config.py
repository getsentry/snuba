"""Reads allocation policy config from the ``allocation_policies.region``
sentry-option and turns it into the per-policy
constructor kwargs that the policy classes already accept.

The format is documented in CONFIG.md next to this file. This module only
translates names and resolves precedence; the policy classes are unchanged.
"""

from collections.abc import Mapping
from functools import cache
from typing import Any

from snuba.configs.configuration import logger
from snuba.query.allocation_policies import (
    IS_ENFORCED,
    IS_PARDONABLE,
    MAX_THREADS,
    AllocationPolicy,
)
from snuba.state.sentry_options import get_option

REGION_KEY = "allocation_policies.region"

POLICIES = "policies"
DELETE_POLICIES = "delete_policies"

# Settings that ``defaults`` and a resource pass down to every policy.
_INHERITED = {"enforce", "pardonable", "max_threads", "thresholds"}

_COMMON_SETTINGS = {
    "enforce": IS_ENFORCED,
    "pardonable": IS_PARDONABLE,
    "max_threads": MAX_THREADS,
}

_CONCURRENCY_SETTINGS = {
    "rate_limit_shard_factor": "rate_limit_shard_factor",
    "max_query_duration_s": "max_query_duration_s",
}

# policy key -> (registered class name, {setting name -> constructor kwarg}).
# Only these names are accepted so a typo is logged instead of silently ignored.
POLICY_SETTINGS: dict[str, tuple[str, dict[str, str]]] = {
    "bytes_scanned_rejecting": (
        "BytesScannedRejectingPolicy",
        {
            "project_bytes_limit": "project_referrer_scan_limit",
            "org_bytes_limit": "organization_referrer_scan_limit",
            "max_bytes_to_read": "organization_max_bytes_to_read",
            "cap_instead_of_reject": "limit_bytes_instead_of_rejecting",
            "timeout_penalty_bytes": "clickhouse_timeout_bytes_scanned_penalization",
            "throttle_bytes_divider": "bytes_throttle_divider",
            "throttle_threads_divider": "threads_throttle_divider",
            "max_bytes_to_read_divider": "max_bytes_to_read_scan_limit_divider",
        },
    ),
    "bytes_scanned_window": (
        "BytesScannedWindowAllocationPolicy",
        {
            "org_bytes_limit": "org_limit_bytes_scanned",
            "throttled_max_threads": "throttled_thread_number",
        },
    ),
    "concurrent_rate_limit": (
        "ConcurrentRateLimitAllocationPolicy",
        {"concurrent_limit": "concurrent_limit", **_CONCURRENCY_SETTINGS},
    ),
    "delete_concurrent_rate_limit": (
        "DeleteConcurrentRateLimitAllocationPolicy",
        {"concurrent_limit": "concurrent_limit", **_CONCURRENCY_SETTINGS},
    ),
    "cross_org": (
        "CrossOrgQueryAllocationPolicy",
        {"referrer_limits": "cross_org_referrer_limits", **_CONCURRENCY_SETTINGS},
    ),
    "referrer_guard_rail": (
        "ReferrerGuardRailPolicy",
        {
            "concurrent_limit": "default_concurrent_request_per_referrer",
            "throttle_requests_divider": "requests_throttle_divider",
            "throttle_threads_divider": "threads_throttle_divider",
            **_CONCURRENCY_SETTINGS,
        },
    ),
}


def deep_merge(base: Any, override: Any) -> Any:
    """Maps merge key by key, lists are appended, anything else is replaced.
    Always returns new containers so option values are never mutated."""
    if isinstance(base, Mapping) and isinstance(override, Mapping):
        merged = dict(base)
        for key, value in override.items():
            merged[key] = deep_merge(base[key], value) if key in base else value
        return merged
    if isinstance(base, list) and isinstance(override, list):
        return [*base, *override]
    return override


def matches(match: Mapping[str, Any], tenant_ids: Mapping[str, str | int]) -> bool:
    return all(key in tenant_ids and tenant_ids[key] in expected for key, expected in match.items())


def _get_config() -> dict[str, Any]:
    config: dict[str, Any] = get_option(REGION_KEY, {})
    if not isinstance(config, dict):
        return {}
    return config


def _pick_inherited(level: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in level.items() if key in _INHERITED}


@cache
def _value_types(class_name: str) -> dict[str, type]:
    # Configuration rejects a default whose type isn't exactly value_type, and the
    # options store 2 for a float config as an int, so values are cast up front.
    definitions = (
        AllocationPolicy.get_from_name(class_name).create_minimal_instance("_").config_definitions()
    )
    return {name: definition.value_type for name, definition in definitions.items()}


def _to_kwargs(
    policy_key: str, settings: Mapping[str, Any], resource: str
) -> tuple[str, dict[str, Any]] | None:
    if policy_key not in POLICY_SETTINGS:
        logger.warning("Unknown allocation policy %s for %s", policy_key, resource)
        return None
    class_name, renames = POLICY_SETTINGS[policy_key]
    renames = {**_COMMON_SETTINGS, **renames}
    value_types = _value_types(class_name)

    kwargs: dict[str, Any] = {}
    for setting, value in settings.items():
        if setting == "thresholds":
            kwargs["thresholds"] = value
        elif setting in renames:
            kwarg = renames[setting]
            kwargs[kwarg] = value_types[kwarg](value) if kwarg in value_types else value
        else:
            logger.warning(
                "Ignoring unknown setting %s on allocation policy %s for %s",
                setting,
                policy_key,
                resource,
            )
    return class_name, kwargs


def resolve_policy_kwargs(
    resource: str,
    tenant_ids: Mapping[str, str | int],
    kind: str = POLICIES,
) -> dict[str, dict[str, Any]] | None:
    """Returns ``{class name: constructor kwargs}`` for ``resource``, or None when
    the resource isn't in the new options (the caller then uses the legacy option).

    ``kind`` is POLICIES (read queries) or DELETE_POLICIES (delete queries).
    """
    config = _get_config()
    resource_config = config.get("resources", {}).get(resource)
    if not isinstance(resource_config, dict):
        return None

    inherited = deep_merge(
        _pick_inherited(config.get("defaults", {})), _pick_inherited(resource_config)
    )
    settings_by_policy: dict[str, Any] = dict(resource_config.get(kind, {}))

    # More matched keys win, so org+referrer beats org beats referrer whatever the
    # file order. The sort is stable, so ties keep file order and the later one wins.
    matching = [
        override
        for override in resource_config.get("overrides", [])
        if matches(override.get("match", {}), tenant_ids)
    ]
    for override in sorted(matching, key=lambda override: len(override.get("match", {}))):
        for policy_key, settings in override.get(kind, {}).items():
            settings_by_policy[policy_key] = deep_merge(
                settings_by_policy.get(policy_key, {}), settings
            )

    kwargs_by_class: dict[str, dict[str, Any]] = {}
    for policy_key, settings in settings_by_policy.items():
        settings = deep_merge(inherited, settings)
        if not settings.pop("enabled", True):
            continue
        if (converted := _to_kwargs(policy_key, settings, resource)) is not None:
            class_name, kwargs = converted
            kwargs_by_class[class_name] = kwargs
    return kwargs_by_class
