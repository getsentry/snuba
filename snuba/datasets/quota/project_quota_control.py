from contextlib import contextmanager
from typing import Iterator

from snuba.datasets.dataset import QuotaControlPolicy
from snuba.request import Request
from snuba.state.rate_limit import RateLimitStats


class ProjectQuotaControl(QuotaControlPolicy):
    @contextmanager
    def acquire(self, request: Request) -> Iterator[RateLimitStats]:
        yield RateLimitStats(0.1, 1)
