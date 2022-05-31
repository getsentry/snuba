from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters


@dataclass()
class QuerySettings:
    """ "The settings for a specific query, similar to compiler flags provided,
    should be immutable
    """

    turbo: bool = False
    consistent: bool = False
    debug: bool = False
    dry_run: bool = False
    # These mutable fields are here due to legcacy reasons
    # the query pipeline uses them to enforce rate limits using
    # query processors. They should not be here
    rate_limit_params: List[RateLimitParameters] = field(default_factory=list)
    resource_quota: ResourceQuota | None = None

    def set_resource_quota(self, quota: ResourceQuota) -> None:
        # TODO: This is the thing that prevents us from making the class frozen
        # revisit it
        self.resource_quota = quota

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        self.rate_limit_params.append(rate_limit_param)
