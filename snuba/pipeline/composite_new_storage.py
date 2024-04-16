from __future__ import annotations

from typing import Mapping, NamedTuple, Optional, Sequence, Tuple, Union

import sentry_sdk

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clusters.cluster import ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import (
    ClickhouseQueryPlan,
    QueryPlanExecutionStrategy,
    QueryRunner,
    SubqueryProcessors,
)
from snuba.datasets.plans.storage_processing import (
    ClickhouseQueryPlanNew,
    build_best_plan,
)
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.joins.semi_joins import SemiJoinOptimizer
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import (
    ClickhouseQueryProcessor,
    CompositeQueryProcessor,
)
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta
from snuba.web import QueryResult


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


def apply_composite_storage_processors(
    query_plan: CompositeDataSourcePlan, settings: QuerySettings
) -> CompositeQuery:
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
    physical_query: CompositeQuery,
    settings: QuerySettings,
    post_processors: Sequence[ClickhouseQueryProcessor] = [],
) -> CompositeDataSourcePlan:

    plan = CompositeDataSourcePlanner(settings).visit(physical_query.get_from_clause())

    return CompositeDataSourcePlan(
        translated_source=physical_query,
        storage_set_key=plan.storage_set_key,
        root_processors=plan.root_processors,
        aliased_processors=plan.aliased_processors,
    )


def check_sub_query_storage_sets(
    alias_to_query_mappings: Mapping[str, ClickhouseQuery]
) -> None:
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


class CompositeDataSourcePlanner(DataSourceVisitor[CompositeDataSourcePlan, Table]):
    """
    Plans the DataSource (the from clause) of a Composite query by
    visiting each node as the data source can be composite itself.

    At each step an intermediate data structure containing the
    translated query and the query processors to run in the execution
    strategy is produced.
    These intermediate structures are merged together and produce the
    CompositeQueryPlan.
    """

    def __init__(self, query_settings: QuerySettings) -> None:
        self.__settings = query_settings

    def _visit_simple_source(self, data_source: Table) -> CompositeDataSourcePlan:
        # We are never supposed to get here.
        raise ValueError("Cannot translate a simple Entity")

    def _visit_join(self, data_source: JoinClause[Table]) -> CompositeDataSourcePlan:
        best_plans = data_source.accept(JoinPlansBuilder(self.__settings))
        join_visitor = JoinDataSourcePlanner(self.__settings, best_plans)
        plan = data_source.accept(join_visitor)

        assert isinstance(plan.translated_source, JoinClause)
        return CompositeDataSourcePlan(
            translated_source=plan.translated_source,
            aliased_processors=plan.processors,
            storage_set_key=plan.storage_set_key,
        )

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Table]
    ) -> CompositeDataSourcePlan:
        assert isinstance(
            data_source, ProcessableQuery
        ), f"Only subqueries are allowed at query planning stage. {type(data_source)} found."
        query_plan = build_best_plan(data_source, self.__settings)
        return CompositeDataSourcePlan(
            translated_source=query_plan.query,
            root_processors=SubqueryProcessors(
                plan_processors=query_plan.plan_query_processors,
                db_processors=query_plan.db_query_processors,
            ),
            storage_set_key=query_plan.storage_set_key,
        )

    def _visit_composite_query(
        self, data_source: CompositeQuery[Table]
    ) -> CompositeDataSourcePlan:
        query_plan = build_best_plan_for_composite_query(data_source, self.__settings)

        return CompositeDataSourcePlan(
            translated_source=query_plan.translated_source,
            root_processors=query_plan.root_processors,
            aliased_processors=query_plan.aliased_processors,
            storage_set_key=query_plan.storage_set_key,
        )


class JoinPlansBuilder(
    JoinVisitor[Mapping[str, Sequence[ClickhouseQueryPlan]], Entity]
):
    """
    Produces all the viable ClickhouseQueryPlans for each subquery
    in the join.
    """

    def __init__(self, settings: QuerySettings) -> None:
        self.__settings = settings

    def visit_individual_node(
        self, node: IndividualNode[Table]
    ) -> Mapping[str, ClickhouseQueryPlanNew]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Invalid composite query. All nodes must be subqueries."

        plan = build_best_plan(node, self.__settings)

        return {node.alias: plan}

    def visit_join_clause(
        self, node: JoinClause[Table]
    ) -> Mapping[str, Sequence[ClickhouseQueryPlan]]:
        return {
            **node.left_node.accept(self),
            **node.right_node.accept(self),
        }


class JoinQueryVisitor(JoinVisitor[Mapping[str, ClickhouseQuery], Entity]):
    """
    A visitor class responsible for helping traverse a join query and builds
    an alias to physical query mapping.
    """

    def __init__(self, settings: QuerySettings) -> None:
        self.__settings = settings

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Mapping[str, ClickhouseQuery]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Invalid composite query. All nodes must be subqueries."

        entity = get_entity(node.data_source.get_from_clause().key)
        assert isinstance(entity, PluggableEntity)
        entity_processing_executor = entity.get_processing_executor()
        query = entity_processing_executor.execute(node.data_source, self.__settings)

        return {node.alias: query}

    def visit_join_clause(
        self, node: JoinClause[Entity]
    ) -> Mapping[str, ClickhouseQuery]:
        return {
            **node.left_node.accept(self),
            **node.right_node.accept(self),
        }


class JoinDataSourceTransformer(
    JoinVisitor[Union[JoinClause[Table], IndividualNode[Table]], Entity]
):
    """
    A visitor class responsible for producing a join data source.
    """

    def __init__(
        self,
        settings: QuerySettings,
        alias_to_query_mappings: Mapping[str, ClickhouseQuery],
    ) -> None:
        self.__settings = settings
        self.alias_to_query_mappings = alias_to_query_mappings

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> Union[JoinClause[Table], IndividualNode[Table]]:
        assert isinstance(
            node.data_source, ProcessableQuery
        ), "Invalid composite query. All nodes must be subqueries."

        query = self.alias_to_query_mappings[node.alias]
        return IndividualNode(alias=node.alias, data_source=query)

    def visit_join_clause(
        self, node: JoinClause[Entity]
    ) -> Union[JoinClause[Table], IndividualNode[Table]]:
        left_node = node.left_node.accept(self)
        right_node = self.visit_individual_node(node.right_node)

        assert isinstance(right_node, IndividualNode)
        return JoinClause(
            left_node=left_node,
            right_node=right_node,
            keys=node.keys,
            join_type=node.join_type,
            join_modifier=node.join_modifier,
        )


class JoinDataSourcePlanner(JoinVisitor[JoinDataSourcePlan, Table]):
    """
    Plans a join data source inside a composite query producing a
    JoinDataSourcePlan.
    This finds the simple subqueries, plan them with _plan_simple_query
    and keeps a structure containing all the query processors the
    execution strategy will have to run.

    These processors are stored in a mapping with their table alias.
    """

    def __init__(
        self, settings: QuerySettings, plans: Mapping[str, ClickhouseQueryPlanNew]
    ) -> None:
        self.__settings = settings
        self.__plans = plans

    def visit_individual_node(self, node: IndividualNode[Table]) -> JoinDataSourcePlan:
        assert isinstance(
            node.data_source, ProcessableQuery
        ), "Invalid composite query. All nodes must be subqueries."

        sub_query_plan = self.__plans[node.alias]
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
        assert isinstance(
            node.data_source, ClickhouseQuery
        ), "Invalid join structure. Only subqueries are allowed at this stage."
        self.__process_simple_query(
            node.data_source, self.__aliased_processors[node.alias]
        )

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)


class CompositeExecutionStrategy(QueryPlanExecutionStrategy[CompositeQuery[Table]]):
    """
    Executes a composite query after applying the db query processors
    to each subquery.
    """

    def __init__(
        self,
        cluster: ClickhouseCluster,
        root_processors: Sequence[ClickhouseQueryProcessor],
        aliased_processors: Mapping[str, Sequence[ClickhouseQueryProcessor]],
        composite_processors: Sequence[CompositeQueryProcessor],
    ) -> None:
        self.__cluster = cluster
        self.__root_processors = root_processors
        self.__aliased_processors = aliased_processors
        # Processors that are applied to the entire composite query
        # by the execution strategy before running the query on the DB
        # and after all subquery processors have been executed
        self.__composite_processors = composite_processors

    def execute(
        self,
        query: CompositeQuery[Table],
        query_settings: QuerySettings,
        runner: QueryRunner,
    ) -> QueryResult:
        ProcessorsExecutor(
            self.__root_processors, self.__aliased_processors, query_settings
        ).visit(query)

        for p in self.__composite_processors:
            if query_settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "composite_storage_processor", type(p).__name__, query
                ):
                    p.process_query(query, query_settings)
            else:
                p.process_query(query, query_settings)

        return runner(
            clickhouse_query=query,
            query_settings=query_settings,
            reader=self.__cluster.get_reader(),
            cluster_name=self.__cluster.get_clickhouse_cluster_name() or "",
        )


# class CompositeExecutionPipeline(QueryExecutionPipeline):
#     """
#     Executes a logical composite query by generating a plan,
#     applying the plan query processors to all subqueries and
#     handing the result to the execution strategy.
#     """

#     def __init__(
#         self,
#         query: CompositeQuery[Entity],
#         query_settings: QuerySettings,
#         runner: QueryRunner,
#     ):
#         self.__query = query
#         self.__settings = query_settings
#         self.__runner = runner

#     def execute(self) -> QueryResult:
#         plan = CompositeQueryPlanner(self.__query, self.__settings).build_best_plan()
#         root_processors, aliased_processors = plan.get_plan_processors()
#         ProcessorsExecutor(
#             root_processors,
#             aliased_processors,
#             self.__settings,
#         ).visit(plan.query)

#         return plan.execution_strategy.execute(
#             plan.query, self.__settings, self.__runner
#         )
