from typing import Union

from snuba.clickhouse.query import CompositeQuery as ClickhouseComposite
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinNode,
    JoinVisitor,
)
from snuba.query.data_source.simple import Entity, Table
from snuba.query.logical import CompositeQuery as LogicalComposite
from snuba.query.logical import Query as LogicalQuery
from snuba.request.request_settings import RequestSettings


def process_composite_query(
    query: LogicalComposite, request_settings: RequestSettings
) -> ClickhouseComposite:
    """
    Processes a logical composite query into a Clickhouse composite
    query by performing the query processing pipeline of all the
    subqueries.

    After this step the query is supposed to be valid even though
    not optimized.

    TODO: Split the processing of the subqueries to select the
    best combination of query plans after we make the query plan
    builder return multiple plans (one per storage set)
    """

    from_clause = query.get_from_clause()
    processed_from: Union[ClickhouseQuery, ClickhouseComposite, JoinClause[Table]]
    if isinstance(from_clause, LogicalQuery):
        processed_from = _process_simple_query(from_clause, request_settings)
    elif isinstance(from_clause, JoinClause):
        processed_from = _process_join_clause(from_clause, request_settings)
    elif isinstance(from_clause, LogicalComposite):
        processed_from = process_composite_query(from_clause, request_settings)

    return ClickhouseComposite(
        from_clause=processed_from,
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
    )


def _process_join_clause(
    join: JoinClause[Entity], request_settings: RequestSettings
) -> JoinClause[Table]:
    transformed = join.accept(QueryProcessingJoinVisitor(request_settings))
    # The query needs a JoinClause (on purpose), visiting a JoinClause
    # will always generate a JoinClause but the type checker does not
    # know that.
    assert isinstance(
        transformed, JoinClause
    ), f"Invalid join transformation {type(transformed)} produced"
    return transformed


def _process_simple_query(
    query: LogicalQuery, request_settings: RequestSettings
) -> ClickhouseQuery:
    """
    Runs the query processing pipeline of a simple subquery.
    """
    entity = get_entity(query.get_from_clause().key)

    processing_pipeline = entity.get_query_pipeline_builder().build_processing_pipeline(
        query, request_settings
    )
    plan = processing_pipeline.execute()
    return plan.query


class QueryProcessingJoinVisitor(JoinVisitor[JoinNode[Table], Entity]):
    """
    Processes all the joined subqueries by visiting the join structure
    and executing the simple query processing pipeline for each subquery
    found.

    It requires the join to be entirely made of subqueries. Which has
    been guaranteed by the push down phase of the query before this
    step.
    """

    def __init__(self, request_Settings: RequestSettings) -> None:
        self.__settings = request_Settings

    def visit_individual_node(
        self, node: IndividualNode[Entity]
    ) -> IndividualNode[Table]:
        assert isinstance(
            node.data_source, LogicalQuery
        ), "Unprocessable join node. The join must be made of subqueries"
        return IndividualNode(
            node.alias, _process_simple_query(node.data_source, self.__settings)
        )

    def visit_join_clause(self, node: JoinClause[Entity]) -> JoinClause[Table]:
        return JoinClause(
            left_node=node.left_node.accept(self),
            right_node=self.visit_individual_node(node.right_node),
            keys=node.keys,
            join_type=node.join_type,
            join_modifier=node.join_modifier,
        )
