from abc import ABC, abstractmethod
from typing import Optional, Sequence

from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.plans.query_plan import QueryRunner
from snuba.query.query_processor import QueryProcessor
from snuba.request import Request
from snuba.web import QueryResult


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
        self,
        request: Request,
        cluster: ClickhouseCluster,
        runner: QueryRunner,
        db_query_processors: Sequence[QueryProcessor],
    ) -> Optional[QueryResult]:
        """
        Executes and/or splits the request provided, like the equivalent method in
        QueryPlanExecutionStrategy.
        Since not every split algorithm can work on every query, this method should
        return None when the query is not supported by this strategy.

        Implementations are also responsible to execute the DB Query Processors
        provided before running any query on the database.
        """
        raise NotImplementedError
