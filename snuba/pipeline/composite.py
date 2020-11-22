from __future__ import annotations

from dataclasses import dataclass
from typing import Union, Mapping, Optional

from snuba.request.request_settings import RequestSettings
from snuba.web import QueryResult
from snuba.pipeline.query_pipeline import QueryPlanner
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import (
    CompositeQueryPlan,
    SubqueryProcessors,
    QueryPlanExecutionStrategy,
    QueryRunner,
    ClickhouseQueryPlan,
)
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.logical import Query as LogicalQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    JoinClause,
    JoinNode,
    JoinVisitor,
    IndividualNode,
)
from snuba.query import ProcessableQuery


def _plan_simple_query(
    query: ProcessableQuery[Entity], settings: RequestSettings
) -> ClickhouseQueryPlan:
    """
    Plans the sub query relying on the entity planner.
    """

    assert isinstance(
        query, LogicalQuery
    ), f"Only subqueries are allowed at query planning stage. {type(query)} found."

    entity = get_entity(query.get_from_clause().key)

    return entity.get_query_pipeline_builder().build_planner(query, settings).execute()


@dataclass(frozen=True)
class JoinDataSourcePlan:
    """
    Intermediate data structure maintained when visiting and
    transforming the query data source.
    """

    translated_source: JoinNode[Table]
    processors: Mapping[str, SubqueryProcessors]


class JoinSourcePlanner(JoinVisitor[JoinDataSourcePlan, Entity]):
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
        left = node.left_node.accept(self)
        right = self.visit_individual_node(node.right_node)
        # mypy does not know that the method above only produces individual nodes.
        assert isinstance(right.translated_source, IndividualNode)
        return JoinDataSourcePlan(
            translated_source=JoinClause(
                left_node=left.translated_source,
                right_node=right.translated_source,
                keys=node.keys,
                join_type=node.join_type,
                join_modifier=node.join_modifier,
            ),
            processors={**left.processors, **right.processors},
        )


def _plan_composite_query(
    query: CompositeQuery[Entity], settings: RequestSettings
) -> CompositeQueryPlan:
    planned_source = CompositeDataSourcePlanner(settings).visit(query.get_from_clause())

    return CompositeQueryPlan(
        query=CompositeQuery(
            from_clause=planned_source.translated_source,
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
        root_processors=planned_source.root_processors,
        aliased_processors=planned_source.aliased_processors,
    )


@dataclass(frozen=True)
class CompositeDataSourcePlan:
    translated_source: Union[ClickhouseQuery, CompositeQuery[Table], JoinClause[Table]]
    root_processors: Optional[SubqueryProcessors] = None
    aliased_processors: Optional[Mapping[str, SubqueryProcessors]] = None


class CompositeDataSourcePlanner(DataSourceVisitor[CompositeDataSourcePlan, Entity],):
    def __init__(self, request_settings: RequestSettings,) -> None:
        self.__settings = request_settings

    def _visit_simple_source(self, data_source: Entity) -> CompositeDataSourcePlan:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Entity]) -> CompositeDataSourcePlan:
        join_visitor = JoinSourcePlanner(self.__settings)
        plan = data_source.accept(join_visitor)
        assert isinstance(plan.translated_source, JoinClause)
        return CompositeDataSourcePlan(
            translated_source=plan.translated_source,
            aliased_sub_queries_processors=plan.processors,
        )

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> CompositeDataSourcePlan:
        plan = _plan_simple_query(data_source, self.__settings)
        return CompositeDataSourcePlan(
            translated_source=plan.query,
            root_processors=SubqueryProcessors(
                plan_processors=plan.plan_query_processors,
                db_processors=plan.db_query_processors,
            ),
        )

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> CompositeDataSourcePlan:
        plan = _plan_composite_query(data_source, self.__settings)
        return CompositeDataSourcePlan(
            translated_source=plan.query,
            root_processors=plan.root_processors,
            aliased_processors=plan.aliased_processors,
        )


class CompositePlanner(QueryPlanner[CompositeQueryPlan]):
    def __init__(
        self, query: CompositeQuery[Entity], settings: RequestSettings,
    ) -> None:
        self.__query = query
        self.__settings = settings

    def execute(self) -> CompositeQueryPlan:
        return _plan_composite_query(self.__query, self.__settings)


class CompositeExecutionStrategy(QueryPlanExecutionStrategy[CompositeQuery[Table]]):
    def execute(
        self,
        query: CompositeQuery[Table],
        request_settings: RequestSettings,
        runner: QueryRunner,
    ) -> QueryResult:
        raise NotImplementedError
