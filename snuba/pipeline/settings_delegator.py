import copy
from typing import Optional, Sequence

from snuba.request.request_settings import RequestSettings
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters


class RateLimiterDelegate(RequestSettings):
    """
    When we run the pipeline delegator, we are running multiple queries
    at the same time. Generally they are duplicates.
    Each pipeline should not know anything about the environment it runs
    into otherwise we would increase the pipeline complexity
    dramatically.

    Still parallel pipelines on separate clusters should not share the
    same quota and rate limiter.

    The idea is to provide the query pipeline with delegates over the
    components they need to execute it that create separate namespaces.
    Specifically the PipelineDelegator provides a RateLimiterDelegate to
    to each query pipeline so that the pipeline can use a separate
    rate limiter namespace without knowing about it.
    """

    def __init__(self, prefix: str, delegate: RequestSettings):
        self.referrer = delegate.referrer
        self.__delegate = copy.deepcopy(delegate)
        self.__prefix = prefix

    def __append_prefix(self, rate_limiter: RateLimitParameters) -> RateLimitParameters:
        return RateLimitParameters(
            rate_limit_name=rate_limiter.rate_limit_name,
            bucket=f"{self.__prefix}_{rate_limiter.bucket}",
            per_second_limit=rate_limiter.per_second_limit,
            concurrent_limit=rate_limiter.concurrent_limit,
        )

    def get_turbo(self) -> bool:
        return self.__delegate.get_turbo()

    def get_consistent(self) -> bool:
        return self.__delegate.get_consistent()

    def get_debug(self) -> bool:
        return self.__delegate.get_debug()

    def get_parent_api(self) -> str:
        return self.__delegate.get_parent_api()

    def get_dry_run(self) -> bool:
        return self.__delegate.get_dry_run()

    def get_legacy(self) -> bool:
        return self.__delegate.get_legacy()

    def get_team(self) -> str:
        return self.__delegate.get_team()

    def get_feature(self) -> str:
        return self.__delegate.get_feature()

    def get_app_id(self) -> str:
        return self.__delegate.get_app_id()

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return [
            self.__append_prefix(r) for r in self.__delegate.get_rate_limit_params()
        ]

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        self.__delegate.add_rate_limit(rate_limit_param)

    def get_resource_quota(self) -> Optional[ResourceQuota]:
        return self.__delegate.get_resource_quota()

    def set_resource_quota(self, quota: ResourceQuota) -> None:
        self.__delegate.set_resource_quota(quota)
