from typing import List

from snuba.query.allocation_policies import QuotaAllowance


def get_max_bytes_to_read(quota_allowances: List[QuotaAllowance]) -> int:
    max_bytes_to_read = min(
        [qa.max_bytes_to_read for qa in quota_allowances],
        key=lambda mb: float("inf") if mb == 0 else mb,
    )
    if max_bytes_to_read != 0:
        return max_bytes_to_read
    return 0
