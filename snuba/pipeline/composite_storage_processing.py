from __future__ import annotations

from typing import Mapping, NamedTuple, Sequence, Union

import sentry_sdk

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    CompositeQueryPlan,
    SubqueryProcessors,
)
from snuba.datasets.plans.storage_processing import build_best_plan
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.joins.semi_joins import SemiJoinOptimizer
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta


def apply_composite_storage_processors(
    query_plan: CompositeQueryPlan, settings: QuerySettings
) -> CompositeQuery[Table]:
    """
    Given a composite query plan, this function applies all the
    db_processors and plan_processors defined on the plan to the
    physical query.
    """
    assert isinstance(query_plan.translated_source, CompositeQuery)
    plan_root_processors, plan_aliased_processors = query_plan.get_plan_processors()
    ProcessorsExecutor(
        plan_root_processors,
        plan_aliased_processors,
        settings,
    ).visit(query_plan.translated_source)
    root_db_processors, aliased_db_processors = query_plan.get_db_processors()
    ProcessorsExecutor(root_db_processors, aliased_db_processors, settings).visit(
        query_plan.translated_source
    )
    composite_processors = [SemiJoinOptimizer()]
    for p in composite_processors:
        if settings.get_dry_run():
            with explain_meta.with_query_differ(
                "composite_storage_processor",
                type(p).__name__,
                query_plan.translated_source,
            ):
                p.process_query(query_plan.translated_source, settings)
        else:
            p.process_query(query_plan.translated_source, settings)

    return query_plan.translated_source


def build_best_plan_for_composite_query(
    physical_query: CompositeQuery[Table],
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> CompositeQueryPlan:
    """
    A function that traverses each sub-query node in a physical composite query,
    builds a plan for each sub-query, combines them into a single composite plan, and
    returns all the necessary storage processors that need to be applied.
    """
    plan = CompositeDataSourcePlanner(settings).visit(physical_query.get_from_clause())
    return CompositeQueryPlan(
        translated_source=physical_query,
        storage_set_key=plan.storage_set_key,
        root_processors=plan.root_processors,
        aliased_processors=plan.aliased_processors,
    )


def check_sub_query_storage_sets(alias_to_query_mappings: Mapping[str, ClickhouseQuery]) -> None:
    """
    Receives a mapping with all the valid query for each subquery
    in a join, and checks that queries are grouped in JOINABLE_STORAGE_SETS.
    """
    from snuba.datasets.storages.factory import get_storage
    from snuba.pipeline.utils.storage_finder import StorageKeyFinder

    all_storage_sets = []
    for _, query in alias_to_query_mappings.items():
        storage_key = StorageKeyFinder().visit(query)
        storage = get_storage(storage_key)
        all_storage_sets.append(storage.get_storage_set_key())

    is_valid_storage_set_combination(*all_storage_sets)


class CompositeDataSourcePlanner(DataSourceVisitor[CompositeQueryPlan, Table]):
    """
    Plans the DataSource (the from clause) of a Composite query by
    visiting each node as the data source can be composite itself.

    At each step an intermediate data structure containing the
    translated query and the query processors to be executed produced.
    These intermediate structures are merged together and produce the
    CompositeQueryPlan.
    """

    def __init__(self, query_settings: QuerySettings) -> None:
        self.__settings = query_settings

    def _visit_simple_source(self, data_source: Table) -> CompositeQueryPlan:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Table]) -> CompositeQueryPlan:
        best_plans = data_source.accept(JoinPlansBuilder(self.__settings))
        join_visitor = JoinDataSourcePlanner(self.__settings, best_plans)
        plan = data_source.accept(join_visitor)

        assert isinstance(plan.translated_source, JoinClause)
        return CompositeQueryPlan(
            translated_source=plan.translated_source,
            aliased_processors=plan.processors,
            storage_set_key=plan.storage_set_key,
        )

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> CompositeQueryPlan:
        assert isinstance(data_source, ProcessableQuery), (
            f"Only subqueries are allowed at query planning stage. {type(data_source)} found."
        )
        query_plan = build_best_plan(data_source, self.__settings)
        return CompositeQueryPlan(
            translated_source=query_plan.query,
            root_processors=SubqueryProcessors(
                plan_processors=query_plan.plan_query_processors,
                db_processors=query_plan.db_query_processors,
            ),
            storage_set_key=query_plan.storage_set_key,
        )

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> CompositeQueryPlan:
        query_plan = build_best_plan_for_composite_query(data_source, self.__settings)

        return CompositeQueryPlan(
            translated_source=query_plan.translated_source,
            root_processors=query_plan.root_processors,
            aliased_processors=query_plan.aliased_processors,
            storage_set_key=query_plan.storage_set_key,
        )


class JoinPlansBuilder(JoinVisitor[Mapping[str, ClickhouseQueryPlan], Table]):
    """
    Produces all the viable ClickhouseQueryPlans for each subquery
    in the join.
    """

    def __init__(self, settings: QuerySettings) -> None:
        self.__settings = settings

    def visit_individual_node(
        self, node: IndividualNode[Table]
    ) -> Mapping[str, ClickhouseQueryPlan]:
        assert isinstance(node.data_source, ClickhouseQuery), (
            "Invalid composite query. All nodes must be subqueries."
        )

        plan = build_best_plan(node.data_source, self.__settings)

        return {node.alias: plan}

    def visit_join_clause(self, node: JoinClause[Table]) -> Mapping[str, ClickhouseQueryPlan]:
        return {
            **node.left_node.accept(self),
            **node.right_node.accept(self),
        }


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
    storage_set_key: StorageSetKey


class JoinDataSourcePlanner(JoinVisitor[JoinDataSourcePlan, Table]):
    """
    Plans a join data source inside a composite query producing a
    JoinDataSourcePlan.
    This finds the simple subqueries, plan them with _plan_simple_query
    and keeps a structure containing all the query processors the
    execution strategy will have to run.

    These processors are stored in a mapping with their table alias.
    """

    def __init__(self, settings: QuerySettings, plans: Mapping[str, ClickhouseQueryPlan]) -> None:
        self.__settings = settings
        self.__plans = plans

    def visit_individual_node(self, node: IndividualNode[Table]) -> JoinDataSourcePlan:
        assert isinstance(node.data_source, ProcessableQuery), (
            "Invalid composite query. All nodes must be subqueries."
        )

        sub_query_plan = self.__plans[node.alias]
        return JoinDataSourcePlan(
            translated_source=IndividualNode(alias=node.alias, data_source=sub_query_plan.query),
            processors={
                node.alias: SubqueryProcessors(
                    plan_processors=sub_query_plan.plan_query_processors,
                    db_processors=sub_query_plan.db_query_processors,
                )
            },
            storage_set_key=sub_query_plan.storage_set_key,
        )

    def visit_join_clause(self, node: JoinClause[Table]) -> JoinDataSourcePlan:
        left_node = node.left_node.accept(self)
        right_node = self.visit_individual_node(node.right_node)

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
            storage_set_key=left_node.storage_set_key,
        )


class ProcessorsExecutor(DataSourceVisitor[None, Table], JoinVisitor[None, Table]):
    """
    Applies in place the a sequence of query processors to the subqueries
    of a composite query.
    Processors are provided as a sequence of processors to be applied to
    the root subquery or a mapping of sequences (one per alias) to be
    applied to subqueries in a join. In the join case there are multiple
    subqueries with a table alias in the composite query.
    """

    def __init__(
        self,
        root_processors: Sequence[ClickhouseQueryProcessor],
        aliased_processors: Mapping[str, Sequence[ClickhouseQueryProcessor]],
        query_settings: QuerySettings,
    ) -> None:
        self.__root_processors = root_processors
        self.__aliased_processors = aliased_processors
        self.__settings = query_settings

    def __process_simple_query(
        self,
        clickhouse_query: ClickhouseQuery,
        processors: Sequence[ClickhouseQueryProcessor],
    ) -> None:
        for clickhouse_processor in processors:
            with sentry_sdk.start_span(
                description=type(clickhouse_processor).__name__, op="processor"
            ):
                clickhouse_processor.process_query(clickhouse_query, self.__settings)

    def _visit_simple_source(self, data_source: Table) -> None:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Table]) -> None:
        data_source.accept(self)

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> None:
        assert isinstance(data_source, ClickhouseQuery)
        self.__process_simple_query(data_source, self.__root_processors)

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[Table]) -> None:
        assert isinstance(node.data_source, ClickhouseQuery), (
            "Invalid join structure. Only subqueries are allowed at this stage."
        )
        self.__process_simple_query(node.data_source, self.__aliased_processors[node.alias])

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)
