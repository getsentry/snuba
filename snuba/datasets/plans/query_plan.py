from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Generic, Mapping, Optional, Sequence, Tuple, TypeVar, Union

from snuba.clickhouse.query import Query
from snuba.clusters.storage_sets import StorageSetKey
from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.processors.physical import ClickhouseQueryProcessor

TQuery = TypeVar("TQuery", bound=AbstractQuery)


@dataclass(frozen=True)
class QueryPlan(ABC, Generic[TQuery]):
    """
    Provides the directions to execute a Clickhouse Query against one
    storage or multiple joined ones.

    This is produced in the storage processing stage of the query pipeline.

    It embeds the Clickhouse Query (the query to run on the storage
    after translation). It also provides a plan execution strategy
    that takes care of coordinating the execution of the query against
    the database.

    When running a query we need a cluster, the cluster is picked according
    to the storages sets containing the storages used in the query.
    So the plan keeps track of the storage set as well.
    There must be only one storage set per query.
    """

    query: TQuery
    storage_set_key: StorageSetKey


@dataclass(frozen=True)
class ClickhouseQueryPlan(QueryPlan[Union[Query, ProcessableQuery[Table]]]):
    """
    Query plan for a single entity, single storage query.

    It provides the sequence of storage specific QueryProcessors
    to apply to the query after the the storage has been selected.
    These are divided in two sequences: plan processors and DB
    processors.
    Plan processors and DB Query Processors are both executed only
    once per plan.
    """

    # Per https://github.com/python/mypy/issues/10039, this has to be redeclared
    # to avoid a mypy error.
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


from typing import NamedTuple

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.data_source.join import JoinClause


class CompositeQueryPlan(NamedTuple):
    """
    Intermediate query plan data structure maintained when visiting
    each node of a composite query. This structure responsible for keeping track
    of a query and the relavent processors that need to be executed on the query.

    We cannot use a Composite plan itself because the data source type is too
    restrictive and processors are structured differently for composite queries
    (e.g. aliased processors).
    """

    translated_source: Union[
        ClickhouseQuery,
        ProcessableQuery[Table],
        CompositeQuery[Table],
        JoinClause[Table],
    ]
    storage_set_key: StorageSetKey
    root_processors: Optional[SubqueryProcessors] = None
    aliased_processors: Optional[Mapping[str, SubqueryProcessors]] = None

    def get_db_processors(
        self,
    ) -> Tuple[
        Sequence[ClickhouseQueryProcessor],
        Mapping[str, Sequence[ClickhouseQueryProcessor]],
    ]:
        return (
            (
                self.root_processors.db_processors
                if self.root_processors is not None
                else []
            ),
            (
                {
                    alias: subquery.db_processors
                    for alias, subquery in self.aliased_processors.items()
                }
                if self.aliased_processors is not None
                else {}
            ),
        )

    def get_plan_processors(
        self,
    ) -> Tuple[
        Sequence[ClickhouseQueryProcessor],
        Mapping[str, Sequence[ClickhouseQueryProcessor]],
    ]:
        return (
            (
                self.root_processors.plan_processors
                if self.root_processors is not None
                else []
            ),
            (
                {
                    alias: subquery.plan_processors
                    for alias, subquery in self.aliased_processors.items()
                }
                if self.aliased_processors is not None
                else {}
            ),
        )
