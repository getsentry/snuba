from contextlib import contextmanager
from typing import Iterator, Optional

from snuba.clickhouse.query_dsl.accessors import ProjectsFinder
from snuba.datasets.dataset import QuotaControlPolicy, QuotaExceeded
from snuba.request import Request
from snuba.state import get_config, get_configs
from snuba.state.rate_limit import (
    QUOTA_RATE_LIMIT_NAME,
    RateLimitExceeded,
    RateLimitParameters,
    RateLimitStats,
    rate_limit,
)

QUOTA_ENFORCEMENT_ENABLED = "quota_enforcement_enabled"


class ProjectQuotaControl(QuotaControlPolicy):
    """
    Enforces a project specific quota.
    The quota is defined as queries per second and concurrent queries.
    The rate limiter based on Redis is used to enforce the quota.
    The quota is defined per project in the runtime configuration.

    If no suitable project condition is found in the query, the check
    is bypassed.
    """

    def __init__(self, project_column: str):
        self.__project_columns = project_column

    @contextmanager
    def acquire(self, request: Request) -> Iterator[Optional[RateLimitStats]]:
        quota_enforcement_enabled = get_config(QUOTA_ENFORCEMENT_ENABLED, 0)
        project_ids = ProjectsFinder(self.__project_columns).visit(request.query)
        print(type(quota_enforcement_enabled))
        if quota_enforcement_enabled and project_ids:
            # TODO: Use all the projects, not just one
            project_id = project_ids.pop()

            prl, pcl = get_configs(
                [("project_per_second_limit", 1000), ("project_concurrent_limit", 1000)]
            )

            # Specific projects can have their rate limits overridden
            (per_second, concurr) = get_configs(
                [
                    ("project_per_second_limit_{}".format(project_id), prl),
                    ("project_concurrent_limit_{}".format(project_id), pcl),
                ]
            )

            try:
                with rate_limit(
                    RateLimitParameters(
                        rate_limit_name=QUOTA_RATE_LIMIT_NAME,
                        bucket=str(project_id),
                        per_second_limit=per_second,
                        concurrent_limit=concurr,
                    )
                ) as stats:
                    yield stats
            except RateLimitExceeded as cause:
                raise QuotaExceeded(str(cause)) from cause
        else:
            yield None
