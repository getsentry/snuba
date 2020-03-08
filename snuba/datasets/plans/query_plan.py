from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Callable, NamedTuple, Sequence

from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.reader import Result
from snuba.request import RequestSettings


class RawQueryResult(NamedTuple):
    result: Result
    extra: Any


SingleQueryRunner = Callable[[Query, RequestSettings], RawQueryResult]


class QueryPlanExecutionStrategy(ABC):
    """
    Orchestrate the executions of a query. An example use case is split
    queries, when the split is selected by the storage.
    It does not know how to run a query against a DB, but it knows how
    and whether to break down a query and how to assemble the results back.
    It receives a runner, that takes care of actually executing one statement
    against the DB.
    """

    def execute(
        self, query: Query, settings: RequestSettings, runner: SingleQueryRunner
    ) -> RawQueryResult:
        raise NotImplementedError


@dataclass(frozen=True)
class StorageQueryPlan:
    """
    Provides the directions to execute the query against one storage
    or multiple joined ones.
    This is produced by StorageQueryPlanBuilder (provided by the dataset)
    after the dataset query processing has been performed and the storage
    has been selected.
    It embeds the storage query class, the sequence of QueryProcessors to
    apply to the storage query and a plan executor, which orchestrate the
    query in case the query has to be split into multiple and the results
    composed back.
    """

    query_processors: Sequence[QueryProcessor]
    storage_query: Query
    plan_executor: QueryPlanExecutionStrategy


class StorageQueryPlanBuilder(ABC):
    """
    Embeds the dataset specific logic that selects what storage to use
    to execute the query and produce the storage query (when we will
    have a separation between Snuba Query and Storage Query).
    This is provided by a dataset and, when executed, it returns a
    StorageQueryPlan the api is able to understand.
    """

    def build_plan(self, query: Query, settings: RequestSettings) -> StorageQueryPlan:
        raise NotImplementedError
