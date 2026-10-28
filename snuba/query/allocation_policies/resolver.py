from collections.abc import Iterator, Mapping
from typing import Any

from snuba.configs.configuration import ResourceIdentifier, logger
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    ALLOCATION_POLICY_KEY,
    AllocationPolicy,
    PassthroughPolicy,
    QueryType,
)
from snuba.query.allocation_policies.policy_config import (
    DELETE_POLICIES,
    POLICIES,
    matches,
    resolve_policy_kwargs,
)
from snuba.state.sentry_options import get_mapped_option
from snuba.utils.registered_class import InvalidConfigKeyError


def _apply_block(
    block: Mapping[str, Any],
    specs_by_name: dict[str, Mapping[str, Any]],
    resource: str,
) -> None:
    for name in block.get("remove") or []:
        specs_by_name.pop(name, None)

    specs = block.get("policies", [])
    if not isinstance(specs, list):
        logger.warning(
            "Ignoring allocation_policy block without policies list for %s: %r",
            resource,
            block,
        )
        return

    for spec in specs:
        if not isinstance(spec, dict):
            logger.warning(
                "Ignoring malformed allocation_policy entry for %s: %r",
                resource,
                spec,
            )
            continue

        name = spec.get("name", None)
        if not isinstance(name, str):
            logger.warning(
                "Ignoring allocation_policy entry without name for %s: %r",
                resource,
                spec,
            )
            continue
        if thresholds := block.get("thresholds", {}):
            spec = {**spec, "thresholds": thresholds}
        specs_by_name[name] = spec


def _resolve_policy_specs(
    blocks: list[Mapping[str, Any]],
    tenant_ids: Mapping[str, str | int],
    resource: str,
) -> dict[str, Mapping[str, Any]]:
    specs_by_name: dict[str, Mapping[str, Any]] = {}
    for block in blocks:
        if not isinstance(block, dict):
            logger.warning(
                "Ignoring malformed allocation_policy block for %s: %r",
                resource,
                block,
            )
            continue

        match = block.get("match", {})
        if not isinstance(match, dict) or not matches(match, tenant_ids):
            continue
        _apply_block(block, specs_by_name, resource)
    return specs_by_name


def _construct_policies(
    specs_by_name: Mapping[str, Mapping[str, Any]],
    resource: str,
    query_type: QueryType | None = None,
) -> Iterator[AllocationPolicy]:
    for name, spec in specs_by_name.items():
        try:
            policy = AllocationPolicy.get_from_name(name).from_kwargs(
                storage_key=resource,
                **spec,
            )
        except InvalidConfigKeyError:
            logger.warning("Unknown allocation policy %s for %s", name, resource)
            continue
        if query_type is not None and policy.query_type is not query_type:
            logger.warning(
                "Ignoring allocation policy %s for %s: it limits %s queries, not %s",
                name,
                resource,
                policy.query_type.value,
                query_type.value,
            )
            continue
        yield policy


def get_active_allocation_policies(
    resource_identifier: ResourceIdentifier,
    tenant_ids: Mapping[str, str | int] | None = None,
) -> list[AllocationPolicy]:
    """Build the AllocationPolicy list configured for ``resource_identifier``.

    A resource listed in the ``allocation_policies.region`` option is
    resolved from it (see policy_config.py and CONFIG.md); policies there that
    limit delete queries are skipped. Otherwise this falls back to the legacy
    format below.

    Reads the ``allocation_policies`` sentry-option (keyed by ResourceIdentifier
    value). Each resource is a list of match blocks. Matching blocks (AND of
    present ``match`` keys against ``tenant_ids``; ``match: {}`` always matches)
    are applied in file order: ``remove`` then add/replace by ``name``. An
    absent or empty result falls back to a PassthroughPolicy. Per-policy
    settings on the item (is_enforced, concurrent_limit, …) are passed as
    constructor kwargs.
    """
    resource = resource_identifier.value
    kwargs_by_class = resolve_policy_kwargs(resource, tenant_ids or {}, POLICIES)
    if kwargs_by_class is not None:
        policies = list(_construct_policies(kwargs_by_class, resource, QueryType.SELECT))
    else:
        specs_by_name = _resolve_policy_specs(
            get_mapped_option(ALLOCATION_POLICY_KEY, resource, []),
            tenant_ids or {},
            resource,
        )
        policies = list(_construct_policies(specs_by_name, resource))
    if not policies:
        policies = [PassthroughPolicy(ResourceIdentifier(StorageKey(resource)))]
    return policies


def get_active_delete_allocation_policies(
    resource_identifier: ResourceIdentifier,
    tenant_ids: Mapping[str, str | int] | None = None,
) -> list[AllocationPolicy]:
    """Build the delete-query policies configured under ``delete_policies`` in the
    ``allocation_policies.region`` option. Empty when none are set."""
    resource = resource_identifier.value
    kwargs_by_class = resolve_policy_kwargs(resource, tenant_ids or {}, DELETE_POLICIES)
    return list(_construct_policies(kwargs_by_class or {}, resource, QueryType.DELETE))
