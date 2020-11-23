from __future__ import annotations

from typing import Mapping, NamedTuple, Optional, Union

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    QueryPlanExecutionStrategy,
    QueryRunner,
    SubqueryProcessors,
)
from snuba.pipeline.query_pipeline import QueryPlanner
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.logical import Query as LogicalQuery
from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult


class CompositeQueryPlanner(QueryPlanner[CompositeQueryPlan]):
    """
    A QueryPlanner that produces a plan for composite queries.
    This is supposed to be used by the dataset query execution pipeline
    for composite queries.

    It translates the query with all subqueries and keeps track of all
    query processors that have to be executed on each subquery during
    the execution strategy just as it happens for simple queries.

    The translation of the subqueries is performed through the query
    planner of each involved entity, while the translation of the composite
    query itself is a simple identity (there are no translation rules
    there as of now).

    This is not customizable as of now. It is not possible to provide
    a dedicated query planner for a specific entity. But that would
    be fairly simple to implement.
    """

    def __init__(
        self, query: CompositeQuery[Entity], settings: RequestSettings,
    ) -> None:
        self.__query = query
        self.__settings = settings

    def execute(self) -> CompositeQueryPlan:
        return _plan_composite_query(self.__query, self.__settings)


def _plan_composite_query(
    query: CompositeQuery[Entity], settings: RequestSettings
) -> CompositeQueryPlan:
    """
    Produces a composite query plan out of a composite query.

    This is the bulk of the logic of The Composite Planner. It is kept
    in its own function because it needs to be used by the data source
    visitor when planning subqueries (which can be composite as well).
    """

    planned_data_source = CompositeDataSourcePlanner(settings).visit(
        query.get_from_clause()
    )

    return CompositeQueryPlan(
        # This is a mypy issue: https://github.com/python/mypy/issues/7520
        # At the time of writing generics in dataclasses are not properly
        # supported and mypy expects TQuery instead of CompositeQuery here.
        # If the issue is not fixed before we start enforcing this we will
        # have to restructure the query plan.
        query=CompositeQuery(
            from_clause=planned_data_source.translated_source,
            selected_columns=query.get_selected_columns_from_ast(),
            array_join=query.get_arrayjoin_from_ast(),
            condition=query.get_condition_from_ast(),
            groupby=query.get_groupby_from_ast(),
            having=query.get_having_from_ast(),
            order_by=query.get_orderby_from_ast(),
            limitby=query.get_limitby(),
            limit=query.get_limit(),
            offset=query.get_offset(),
            totals=query.has_totals(),
            granularity=query.get_granularity(),
        ),
        execution_strategy=CompositeExecutionStrategy(),
        root_processors=planned_data_source.root_processors,
        aliased_processors=planned_data_source.aliased_processors,
    )


def _plan_simple_query(
    query: ProcessableQuery[Entity], settings: RequestSettings
) -> ClickhouseQueryPlan:
    """
    Uses the Entity query planner to plan a simple single entity subquery.
    This means translating the query and identifying all the query
    processors we need to apply.
    """

    assert isinstance(
        query, LogicalQuery
    ), f"Only subqueries are allowed at query planning stage. {type(query)} found."

    return (
        get_entity(query.get_from_clause().key)
        .get_query_pipeline_builder()
        .build_planner(query, settings)
        .execute()
    )


class JoinDataSourcePlan(NamedTuple):
    """
    Intermediate data structure maintained when visiting and
    transforming a join in the composite query data source.

    We cannot use a Composite plan itself as an intermediate
    structure thus the need of this class.
    This is because the data source type is a bit less
    restrictive in this class and we would have to instantiate
    an execution strategy that is not used.
    """

    translated_source: Union[JoinClause[Table], IndividualNode[Table]]
    processors: Mapping[str, SubqueryProcessors]


class JoinDataSourcePlanner(JoinVisitor[JoinDataSourcePlan, Entity]):
    """
    Plans a join data source inside a composite query producing a
    JoinDataSourcePlan.
    This finds the simple subqueries, plan them with _plan_simple_query
    and keeps a structure containing all the query processors the
    execution strategy will have to run.

    These processors are stored in a mapping with their table alias.
    """

    def __init__(self, settings: RequestSettings) -> None:
        self.__settings = settings

    def visit_individual_node(self, node: IndividualNode[Entity]) -> JoinDataSourcePlan:
        assert isinstance(
            node.data_source, ProcessableQuery
        ), "Invalid composite query. All nodes must be subqueries."

        sub_query_plan = _plan_simple_query(node.data_source, self.__settings)
        return JoinDataSourcePlan(
            translated_source=IndividualNode(
                alias=node.alias, data_source=sub_query_plan.query
            ),
            processors={
                node.alias: SubqueryProcessors(
                    plan_processors=sub_query_plan.plan_query_processors,
                    db_processors=sub_query_plan.db_query_processors,
                )
            },
        )

    def visit_join_clause(self, node: JoinClause[Entity]) -> JoinDataSourcePlan:
        left_node = node.left_node.accept(self)
        right_node = self.visit_individual_node(node.right_node)

        # mypy does not know that the method above only produces individual nodes.
        assert isinstance(right_node.translated_source, IndividualNode)
        return JoinDataSourcePlan(
            translated_source=JoinClause(
                left_node=left_node.translated_source,
                right_node=right_node.translated_source,
                keys=node.keys,
                join_type=node.join_type,
                join_modifier=node.join_modifier,
            ),
            processors={**left_node.processors, **right_node.processors},
        )


class CompositeDataSourcePlan(NamedTuple):
    """
    Intermediate data structure maintained when visiting and
    transforming the query data source.

    We cannot use a Composite plan itself as an intermediate
    structure thus the need of this class.
    This is because the data source type is a bit less
    restrictive in this class and we would have to instantiate
    an execution strategy that is not used.
    """

    translated_source: Union[ClickhouseQuery, CompositeQuery[Table], JoinClause[Table]]
    root_processors: Optional[SubqueryProcessors] = None
    aliased_processors: Optional[Mapping[str, SubqueryProcessors]] = None

    @classmethod
    def from_simple_query_plan(
        cls, query_plan: ClickhouseQueryPlan
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan(
            translated_source=query_plan.query,
            root_processors=SubqueryProcessors(
                plan_processors=query_plan.plan_query_processors,
                db_processors=query_plan.db_query_processors,
            ),
        )

    @classmethod
    def from_composite_subquery(
        cls, query_plan: CompositeQueryPlan
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan(
            translated_source=query_plan.query,
            root_processors=query_plan.root_processors,
            aliased_processors=query_plan.aliased_processors,
        )


class CompositeDataSourcePlanner(DataSourceVisitor[CompositeDataSourcePlan, Entity]):
    """
    Plans the DataSource (the from clause) of a Composite query by
    visiting each node as the data source can be composite itself.

    At each step an intermediate data structure containing the
    translated query and the query processors to run in the execution
    strategy is produced.
    These intermediate structures are merged together and produce the
    CompositeQueryPlan.
    """

    def __init__(self, request_settings: RequestSettings) -> None:
        self.__settings = request_settings

    def _visit_simple_source(self, data_source: Entity) -> CompositeDataSourcePlan:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Entity]) -> CompositeDataSourcePlan:
        join_visitor = JoinDataSourcePlanner(self.__settings)
        plan = data_source.accept(join_visitor)

        assert isinstance(plan.translated_source, JoinClause)
        return CompositeDataSourcePlan(
            translated_source=plan.translated_source,
            aliased_processors=plan.processors,
        )

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan.from_simple_query_plan(
            _plan_simple_query(data_source, self.__settings)
        )

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> CompositeDataSourcePlan:
        return CompositeDataSourcePlan.from_composite_subquery(
            _plan_composite_query(data_source, self.__settings)
        )


class CompositeExecutionStrategy(QueryPlanExecutionStrategy[CompositeQuery[Table]]):
    def execute(
        self,
        query: CompositeQuery[Table],
        request_settings: RequestSettings,
        runner: QueryRunner,
    ) -> QueryResult:
        # We still need to make the runner take composite queries.
        raise NotImplementedError
