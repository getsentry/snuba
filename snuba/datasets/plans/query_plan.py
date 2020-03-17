from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Sequence

from snuba.query import RawQueryResult
from snuba.query.query_processor import QueryProcessor
from snuba.request import Request


QueryRunner = Callable[[Request], RawQueryResult]


@dataclass(frozen=True)
class StorageQueryPlan:
    """
    Provides the directions to execute the query against one storage
    or multiple joined ones.
    This is produced by StorageQueryPlanBuilder (provided by the dataset)
    after the dataset query processing has been performed and the storage
    has been selected.
    It embeds the sequence of storage specific QueryProcessors to apply
    to the query after the the storage has been selected.
    It also provides a plan execution strategy, in case the query is not
    one individual query statement (like for split queries).
    """

    # TODO: When we will have a separate Query class for Snuba Query and
    # Storage QUery, this plan will also provide the Storage Query. Right
    # now the storage query is the same mutable object referenced by Request
    # so no need to add an additional reference here (it would make the query
    # execution code more confusing).
    query_processors: Sequence[QueryProcessor]
    execution_strategy: QueryPlanExecutionStrategy


class QueryPlanExecutionStrategy(ABC):
    """
    Orchestrate the executions of a query. An example use case is split
    queries, when the split is done at storage level.
    It does not know how to run a query against a DB, but it knows how
    and whether to break down a query and how to assemble the results back.
    It receives a runner, that takes care of actually executing one statement
    against the DB.
    Potentially this could be agnostic to the DB.
    """

    @abstractmethod
    def execute(self, request: Request, runner: QueryRunner) -> RawQueryResult:
        """
        Executes the query plan. The request parameter provides query and query settings.
        The runner parameters is a function to actually run one individual query on the
        database.
        """
        raise NotImplementedError


class StorageQueryPlanBuilder(ABC):
    """
    Embeds the dataset specific logic that selects what storage to use
    to execute the query and produce the storage query (when we will
    have a separation between Snuba Query and Storage Query).
    This is provided by a dataset and, when executed, it returns a
    StorageQueryPlan the api is able to understand.
    """

    @abstractmethod
    def build_plan(self, request: Request) -> StorageQueryPlan:
        raise NotImplementedError
