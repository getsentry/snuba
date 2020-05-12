from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.plans.query_plan import QueryPlanBuilder as LogicalQueryPlanBuilder
from snuba.datasets.plans.query_plan import QueryPlanExecutionStrategy
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

QueryRunner = Callable[[Query, RequestSettings, Reader[SqlQuery]], QueryResult]


@dataclass(frozen=True)
class QueryPlan:
    """
    Provides the directions to execute a Clickhouse Query against one storage
    or multiple joined ones.
    This is produced by QueryPlanBuilder (provided by the dataset) after the dataset
    query processing has been performed and the storage/s has/have been selected.
    It embeds the Clickhouse Query (the query to run on the storage after translation),
    and the sequence of storage specific QueryProcessors to apply to the query after
    the the storage has been selected. These have to be executed only once per plan
    contrarily to the DB Query Processors, still provided by the storage, which
    are executed by the execution strategy at every DB Query.
    It also provides a plan execution strategy that takes care of coordinating the
    execution of the query against the database. The execution strategy can also decide
    to split the query into multiple chunks.
    """

    query: Query
    plan_processors: Sequence[QueryProcessor]
    execution_strategy: QueryPlanExecutionStrategy[Query, QueryRunner]


class QueryPlanBuilder(LogicalQueryPlanBuilder[QueryPlan], ABC):
    """
    Produces a Query Plan to run a query on Clickhouse.
    """

    @abstractmethod
    def build_plan(self, request: Request) -> QueryPlan:
        raise NotImplementedError
