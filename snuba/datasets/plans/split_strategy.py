from abc import ABC, abstractmethod
from typing import Optional

from snuba.datasets.plans.query_plan import QueryRunner
from snuba.request import Request
from snuba.web import RawQueryResult


class StorageQuerySplitStrategy(ABC):
    """
    Implements a query split algorithm. It works in a similar way as a
    QueryExecutionStrategy, it takes a Request and a QueryRunner and decides if it
    can split the query into more efficient parts.
    If it can split the query it uses the QueryRunner to execute every chunk, otherwise
    it immediately returns None.
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
