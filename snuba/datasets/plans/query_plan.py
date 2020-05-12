from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult

TStorageQuery = TypeVar("TStorageQuery")
TQueryRunner = TypeVar("TQueryRunner")


class QueryPlanExecutionStrategy(ABC, Generic[TStorageQuery, TQueryRunner]):
    """
    Orchestrates the executions of a storage query. An example use case is split
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
        self,
        query: TStorageQuery,
        request_settings: RequestSettings,
        runner: TQueryRunner,
    ) -> QueryResult:
        """
        Executes the Query passed in as parameter.
        The runner parameter is a function that actually runs one individual query on the
        database.
        """
        raise NotImplementedError


TQueryPlan = TypeVar("TQueryPlan")


class QueryPlanBuilder(ABC, Generic[TQueryPlan]):
    """
    Embeds the dataset specific logic that selects which storage to use
    to execute the query and produces the storage query.
    This is provided by a dataset and, when executed, it returns a
    query plan that embeds what is needed to run the storage query. The query
    plan class depends on the storage implementation.
    """

    @abstractmethod
    def build_plan(self, request: Request) -> TQueryPlan:
        raise NotImplementedError
