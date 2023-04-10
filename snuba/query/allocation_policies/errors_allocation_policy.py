from __future__ import annotations

import logging
import time

from snuba.query.allocation_policies import (
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.state import get_config
from snuba.state.sliding_windows import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)


class ErrorsAllocationPolicy(AllocationPolicy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rate_limiter = RedisSlidingWindowRateLimiter()

    @property
    def rate_limit_prefix(self) -> str:
        return self.__class__.__name__

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]):
        is_active = get_config(f"{self.rate_limit_prefix}.is_active", True)
        # TODO: This kind of killswitch should just be included with every allocation policy
        if not is_active:
            return DEFAULT_PASSTHROUGH_POLICY._get_quota_allowance(tenant_ids)
        # TODO: figure out shit
        org_scan_limit = get_config(f"{self.rate_limit_prefix}.org_scan_limit", 10000)

        self._rate_limiter.check_within_quotas(
            [
                RequestedQuota(
                    self.rate_limit_prefix,
                    # request a big number because we don't know how much we actually
                    # will use in this query. this doesn't use up any quota
                    int(1e10),
                    [
                        Quota(
                            window_seconds=(15 * 60),
                            granularity_seconds=60,
                            limit=int(org_scan_limit),  # type: ignore
                            prefix_override=f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                        )
                    ],
                ),
            ]
        )
        return QuotaAllowance(True, 8, {})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        if result_or_error.error:
            return
        query_result = result_or_error.query_result
        assert query_result is not None
        bytes_scanned = query_result.result["profile"]["bytes"]
        if not bytes_scanned:
            logging.error("No bytes scanned in query_result")
        org_scan_limit = get_config(f"{self.rate_limit_prefix}.org_scan_limit", 10000)
        # we can assume that the requested quota was granted (because it was)
        # we just need to update the quota with however many bytes were consumed
        self._rate_limiter.use_quotas(
            [
                RequestedQuota(
                    "{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                    bytes_scanned,
                    [
                        Quota(
                            window_seconds=(15 * 60),
                            granularity_seconds=60,
                            limit=int(org_scan_limit),  # type: ignore
                            prefix_override=f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                        )
                    ],
                )
            ],
            grants=[
                GrantedQuota(
                    "{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                    granted=bytes_scanned,
                    reached_quotas=[],
                )
            ],
            timestamp=time.time(),
        )
