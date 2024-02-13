from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Mapping, Optional, Protocol, Sequence, Tuple, TypeVar, Union

from snuba.clickhouse.query import Query
from snuba.clusters.storage_sets import StorageSetKey
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.reader import Reader
from snuba.web import QueryResult


class QueryRunner(Protocol):
    def __call__(
        self,
        clickhouse_query: Union[Query, CompositeQuery[Table]],
        query_settings: QuerySettings,
        reader: Reader,
        cluster_name: str,
    ) -> QueryResult:
        ...


TQuery = TypeVar("TQuery", bound=AbstractQuery)


@dataclass(frozen=True)
class QueryPlan(ABC, Generic[TQuery]):
    """
    Provides the directions to execute a Clickhouse Query against one
    storage or multiple joined ones.

    This is produced by a QueryPlanner class (provided either by the
    dataset or by an entity) after the dataset query processing has
    been performed and the storage/s has/have been selected.

    It embeds the Clickhouse Query (the query to run on the storage
    after translation). It also provides a plan execution strategy
    that takes care of coordinating the execution of the query against
    the database. The execution strategy can also decide to split the
    query into multiple chunks.

    When running a query we need a cluster, the cluster is picked according
    to the storages sets containing the storages used in the query.
    So the plan keeps track of the storage set as well.
    There must be only one storage set per query.

    TODO: Bring the methods that apply the processors in the plan itself.
    Now that we have two Plan implementations with a different
    structure, all code depending on this would have to be written
    differently depending on the implementation.
    Having the methods inside the plan would make this simpler.
    """

    query: TQuery
    execution_strategy: QueryPlanExecutionStrategy[TQuery]
    storage_set_key: StorageSetKey


@dataclass(frozen=True)
class ClickhouseQueryPlan(QueryPlan[Query]):
    """
    Query plan for a single entity, single storage query.

    It provides the sequence of storage specific QueryProcessors
    to apply to the query after the the storage has been selected.
    These are divided in two sequences: plan processors and DB
    processors.
    Plan processors have to be executed only once per plan contrarily
    to the DB Query Processors, still provided by the storage, which
    are executed by the execution strategy at every DB Query.
    """

    # Per https://github.com/python/mypy/issues/10039, this has to be redeclared
    # to avoid a mypy error.
    execution_strategy: QueryPlanExecutionStrategy[Query]
    plan_query_processors: Sequence[ClickhouseQueryProcessor]
    db_query_processors: Sequence[ClickhouseQueryProcessor]


@dataclass(frozen=True)
class SubqueryProcessors:
    """
    Collects the query processors to execute on each subquery
    in a composite query.
    """

    plan_processors: Sequence[ClickhouseQueryProcessor]
    db_processors: Sequence[ClickhouseQueryProcessor]


@dataclass(frozen=True)
class CompositeQueryPlan(QueryPlan[CompositeQuery[Table]]):
    """
    Query plan for the execution of a Composite Query.

    This contains the composite query, the composite strategy and
    the processors for all the simple subqueries.
    SubqueryProcessors instances contain all the storage query
    processors that still have to be executed on the subquery.
    """

    # Per https://github.com/python/mypy/issues/10039, this has to be redeclared
    # to avoid a mypy error.
    execution_strategy: QueryPlanExecutionStrategy[CompositeQuery[Table]]
    # If there is no join there would be no table alias and one
    # single simple subquery.
    root_processors: Optional[SubqueryProcessors]
    # If there is a join, there is one SubqueryProcessors instance
    # per table alias.
    aliased_processors: Optional[Mapping[str, SubqueryProcessors]]

    def __post_init__(self) -> None:
        # If both root processors and aliased processors are populated
        # we have an invalid query. The two are exclusive.
        assert (
            self.root_processors is not None or self.aliased_processors is not None
        ) and not (
            self.root_processors is not None and self.aliased_processors is not None
        )

    def get_plan_processors(
        self,
    ) -> Tuple[
        Sequence[ClickhouseQueryProcessor],
        Mapping[str, Sequence[ClickhouseQueryProcessor]],
    ]:
        """
        Returns the sequences of query processors to execute once per plan.
        This method is used for convenience to unpack the SubqueryProcessors
        objects when executing the query.
        """

        return (
            self.root_processors.plan_processors
            if self.root_processors is not None
            else [],
            {
                alias: subquery.plan_processors
                for alias, subquery in self.aliased_processors.items()
            }
            if self.aliased_processors is not None
            else {},
        )


class QueryPlanExecutionStrategy(ABC, Generic[TQuery]):
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
        self,
        query: TQuery,
        query_settings: QuerySettings,
        runner: QueryRunner,
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
    sequence of valid ClickhouseQueryPlans that embeds what is needed to
    run the storage query.
    """

    @abstractmethod
    def build_and_rank_plans(
        self, query: LogicalQuery, query_settings: QuerySettings
    ) -> Sequence[ClickhouseQueryPlan]:
        """
        Returns all the valid plans for this query sorted in ranking
        order.
        """
        raise NotImplementedError

    def build_best_plan(
        self, query: LogicalQuery, query_settings: QuerySettings
    ) -> ClickhouseQueryPlan:
        plans = self.build_and_rank_plans(query, query_settings)
        assert plans, "Query planner did not produce a plan"
        return plans[0]
