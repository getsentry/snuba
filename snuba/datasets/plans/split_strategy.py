from abc import ABC, abstractmethod
from typing import Callable, Optional

from snuba.clickhouse.query import Query
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

SplitQueryRunner = Callable[[Query, RequestSettings], QueryResult]


class StorageQuerySplitStrategy(ABC):
    """
    Implements a query split algorithm. It works in a similar way as a
    QueryExecutionStrategy, it takes a Request and a query runner and decides if it
    can split the query into more efficient parts.
    If it can split the query, it uses the SplitQueryRunner to execute every chunk, otherwise
    it immediately returns None.

    The main difference between this class and the QueryPlanExecutionStrategy is that
    it relies on a smarter QueryRunner (SplitQueryRunner) than the one provided to the
    execution strategy. The runner this class receives is supposed to take care of
    running the DB query processors before executing the query on the database so that
    the splitter does not to have this responsibility.
    """

    @abstractmethod
    def execute(
        self, query: Query, request_settings: RequestSettings, runner: SplitQueryRunner,
    ) -> Optional[QueryResult]:
        """
        Executes and/or splits the request provided, like the equivalent method in
        QueryPlanExecutionStrategy.
        Since not every split algorithm can work on every query, this method should
        return None when the query is not supported by this strategy.
        """
        raise NotImplementedError
