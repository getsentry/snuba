from collections.abc import Mapping
from typing import Any

from snuba.configs.configuration import ResourceIdentifier, logger
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    ALLOCATION_POLICY_KEY,
    AllocationPolicy,
    PassthroughPolicy,
)
from snuba.state.sentry_options import get_mapped_option
from snuba.utils.registered_class import InvalidConfigKeyError


def _block_matches(match: Mapping[str, Any], tenant_ids: Mapping[str, str | int]) -> bool:
    return all(key in tenant_ids and tenant_ids[key] in expected for key, expected in match.items())


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
        if not isinstance(match, dict) or not _block_matches(match, tenant_ids):
            continue
        _apply_block(block, specs_by_name, resource)
    return specs_by_name


def _construct_policies(
    specs_by_name: Mapping[str, Mapping[str, Any]],
    resource: str,
) -> list[AllocationPolicy]:
    policies: list[AllocationPolicy] = []
    for name, spec in specs_by_name.items():
        try:
            policies.append(
                AllocationPolicy.get_from_name(name).from_kwargs(
                    storage_key=resource,
                    **spec,
                )
            )
        except InvalidConfigKeyError:
            logger.warning("Unknown allocation policy %s for %s", name, resource)
    return policies


def get_active_allocation_policies(
    resource_identifier: ResourceIdentifier,
    tenant_ids: Mapping[str, str | int] | None = None,
) -> list[AllocationPolicy]:
    """Build the AllocationPolicy list configured for ``resource_identifier``.

    Reads the ``allocation_policies`` sentry-option (keyed by ResourceIdentifier
    value). Each resource is a list of match blocks. Matching blocks (AND of
    present ``match`` keys against ``tenant_ids``; ``match: {}`` always matches)
    are applied in file order: ``remove`` then add/replace by ``name``. An
    absent or empty result falls back to a PassthroughPolicy. Per-policy
    settings on the item (is_enforced, concurrent_limit, …) are passed as
    constructor kwargs.
    """
    resource = resource_identifier.value
    specs_by_name = _resolve_policy_specs(
        get_mapped_option(ALLOCATION_POLICY_KEY, resource, []),
        tenant_ids or {},
        resource,
    )
    return _construct_policies(specs_by_name, resource) or [
        PassthroughPolicy(ResourceIdentifier(StorageKey(resource)))
    ]
