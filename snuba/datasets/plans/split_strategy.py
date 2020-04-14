from abc import ABC, abstractmethod
from typing import Optional

from snuba.datasets.plans.query_plan import QueryRunner
from snuba.request import Request
from snuba.web import RawQueryResult


class StorageQuerySplitStrategy(ABC):
    """
    An execution strategy that implements one query split algorithm. For example
    a StorageQuerySplitStrategy can implement the time based query splitting.
    """

    @abstractmethod
    def execute(
        self, request: Request, runner: QueryRunner
    ) -> Optional[RawQueryResult]:
        """
        Executes and splits the request provided, like the equivalent method in
        QueryPlanExecutionStrategy.
        Since not every split algorithm can work on every query, this method should
        return None when the query is not supported by this strategy.
        """
        raise NotImplementedError
