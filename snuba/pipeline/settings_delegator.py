import copy
from typing import Any, MutableMapping, Optional, Sequence

from snuba.query.query_settings import QuerySettings
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters


class RateLimiterDelegate(QuerySettings):
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

    A deepcopy of `delegate` is made to allow multiple concurrent execution
    pipelines to not interfere with each other. The rate limit parameters
    are stored in a list in the RequestSettings object. When the add_rate_limit
    method is called from 2 concurrent execution pipelines they would add
    the rate limits to the same list if a deepcopy is not created.
    """

    def __init__(self, prefix: str, delegate: QuerySettings):
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

    def get_dry_run(self) -> bool:
        return self.__delegate.get_dry_run()

    def get_legacy(self) -> bool:
        return self.__delegate.get_legacy()

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

    def get_clickhouse_settings(self) -> MutableMapping[str, Any]:
        return self.__delegate.get_clickhouse_settings()

    def set_clickhouse_settings(self, settings: MutableMapping[str, Any]) -> None:
        return self.__delegate.set_clickhouse_settings(settings)
