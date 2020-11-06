from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

QueryRunner = Callable[[Query, RequestSettings, Reader], QueryResult]


@dataclass(frozen=True)
class ClickhouseQueryPlan:
    """
    Provides the directions to execute a Clickhouse Query against one storage
    or multiple joined ones.
    This is produced by ClickhouseQueryPlanBuilder (provided by the dataset)
    after the dataset query processing has been performed and the storage/s
    has/have been selected.
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
    execution_strategy: QueryPlanExecutionStrategy


class QueryPlanExecutionStrategy(ABC):
    """
    Orchestrates the executions of a query. An example use case is split
    queries, when the split is done at storage level.
    It does not know how to run a query against a DB, but it knows how
    and whether to break down a query and how to assemble the results back.
    It receives a runner, that takes care of actually executing one statement
    against the DB.
    Implementations are also responsible to execute the DB Query Processors
    provided by the Query Plan Builder before running any query on the database.
    As an example, if the ExecutionStrategy decides to split the query
    into multiple DB queries, DB Query Processors have to be executed for
    each one of them.

    Potentially this could be agnostic to the DB.
    """

    @abstractmethod
    def execute(
        self, query: Query, request_settings: RequestSettings, runner: QueryRunner,
    ) -> QueryResult:
        """
        Executes the Query passed in as parameter.
        The runner parameter is a function that actually runs one individual query on the
        database.
        """
        raise NotImplementedError


class ClickhouseQueryPlanBuilder(ABC):
    """
    Embeds the dataset specific logic that selects which storage to use
    to execute the query and produces the storage query.
    This is provided by a dataset and, when executed, it returns a
    ClickhouseQueryPlan that embeds what is needed to run the storage query.
    """

    @abstractmethod
    def build_plan(self, request: Request) -> ClickhouseQueryPlan:
        raise NotImplementedError
