from collections.abc import Mapping
from typing import TypeVar

from snuba.query.allocation_policies import QuotaAllowance

SCOPED_OVERRIDE_WILDCARD = "*"

T = TypeVar("T")


def resolve_scoped_override(
    scoped: Mapping[str, Mapping[str, T]],
    organization_id: int | str | None,
    referrer: str | None,
    default: T,
) -> T:
    """Resolve a two-level org/referrer-scoped override to a single value.

    ``scoped`` is the value of an object-typed ConfigurableComponent config read
    from ``configurable_component_object_overrides``, shaped as
    ``{organization_id (or "*"): {referrer (or "*"): value}}``. This lets one
    config target a given org, a given referrer, or both at once. Lookups are
    most-specific-first, and the first match wins:

        (org, referrer) > (org, "*") > ("*", referrer) > default

    ``organization_id`` is stringified to match the JSON object keys. A ``None``
    org or referrer simply skips the lookups that would need it.
    """
    org_key = str(organization_id) if organization_id is not None else None
    candidates: list[tuple[str | None, str | None]] = [
        (org_key, referrer),
        (org_key, SCOPED_OVERRIDE_WILDCARD),
        (SCOPED_OVERRIDE_WILDCARD, referrer),
    ]
    for outer, inner in candidates:
        if outer is None or inner is None:
            continue
        inner_map = scoped.get(outer)
        if isinstance(inner_map, Mapping) and inner in inner_map:
            return inner_map[inner]
    return default


def get_max_bytes_to_read(quota_allowances: list[QuotaAllowance]) -> int:
    max_bytes_to_read = min(
        [qa.max_bytes_to_read for qa in quota_allowances],
        key=lambda mb: float("inf") if mb == 0 else mb,
    )
    if max_bytes_to_read != 0:
        return max_bytes_to_read
    return 0
