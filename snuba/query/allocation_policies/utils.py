from snuba.configs.configuration import (  # re-exported for backwards compatibility
    SCOPED_OVERRIDE_WILDCARD,
    resolve_scoped_override,
)
from snuba.query.allocation_policies import QuotaAllowance

__all__ = ["SCOPED_OVERRIDE_WILDCARD", "resolve_scoped_override", "get_max_bytes_to_read"]


def get_max_bytes_to_read(quota_allowances: list[QuotaAllowance]) -> int:
    max_bytes_to_read = min(
        [qa.max_bytes_to_read for qa in quota_allowances],
        key=lambda mb: float("inf") if mb == 0 else mb,
    )
    if max_bytes_to_read != 0:
        return max_bytes_to_read
    return 0
