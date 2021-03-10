import copy
import logging
from functools import partial
from typing import MutableMapping, Union

import sentry_sdk
from snuba import environment
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.querylog import record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.util import with_span
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryResult, transform_column_names
from snuba.web.db_query import raw_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")


class SampleClauseFinder(DataSourceVisitor[bool, Entity], JoinVisitor[bool, Entity]):
    """
    Traverses a query to find FROM clauses that have a sampling
    rate set to check if turbo is set as well.
    """

    def _visit_simple_source(self, data_source: Entity) -> bool:
        return data_source.sample is not None and data_source.sample != 1.0

    def _visit_join(self, data_source: JoinClause[Entity]) -> bool:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Entity]) -> bool:
        return self.visit(data_source.get_from_clause())

    def _visit_composite_query(self, data_source: CompositeQuery[Entity]) -> bool:
        return self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[Entity]) -> bool:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Entity]) -> bool:
        return node.left_node.accept(self) or node.right_node.accept(self)


@with_span()
def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> QueryResult:
    """
    Runs a Snuba Query, then records the metadata about each split query that was run.
    """
    request_copy = copy.deepcopy(request)
    query_metadata = SnubaQueryMetadata(
        request=request_copy,
        dataset=get_dataset_name(dataset),
        timer=timer,
        query_list=[],
    )

    try:
        result = _run_query_pipeline(
            dataset=dataset, request=request, timer=timer, query_metadata=query_metadata
        )
        record_query(request_copy, timer, query_metadata)
    except QueryException as error:
        record_query(request_copy, timer, query_metadata)
        raise error

    return result


def _run_query_pipeline(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
) -> QueryResult:
    """
    Runs the query processing and execution pipeline for a Snuba Query. This means it takes a Dataset
    and a Request and returns the results of the query.

    This process includes:
    - Applying dataset specific syntax extensions (QueryExtension)
    - Applying dataset query processors on the abstract Snuba query.
    - Using the dataset provided ClickhouseQueryPlanBuilder to build a ClickhouseQueryPlan. This step
      transforms the Snuba Query into the Storage Query (that is contextual to the storage/s).
      From this point on none should depend on the dataset.
    - Executing the plan specific query processors.
    - Providing the newly built Query, processors to be run for each DB query and a QueryRunner
      to the QueryExecutionStrategy to actually run the DB Query.
    """
    if not request.settings.get_turbo() and SampleClauseFinder().visit(
        request.query.get_from_clause()
    ):
        metrics.increment("sample_without_turbo", tags={"referrer": request.referrer})

    query_runner = partial(
        _run_and_apply_column_names, timer, query_metadata, request.referrer,
    )

    return (
        dataset.get_query_pipeline_builder()
        .build_execution_pipeline(request, query_runner)
        .execute()
    )


def _run_and_apply_column_names(
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    referrer: str,
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    request_settings: RequestSettings,
    reader: Reader,
) -> QueryResult:
    """
    Executes the query and, after that, replaces the column names in
    QueryResult with the names the user expects and that are stored in
    the SelectedExpression objects in the Query.
    This happens so that we can remove aliases from the Query AST since
    those aliases now are needed to produce the names the user expects
    in the output.
    """

    result = _format_storage_query_and_run(
        timer, query_metadata, referrer, clickhouse_query, request_settings, reader,
    )

    alias_name_mapping: MutableMapping[str, str] = {}
    for select_col in clickhouse_query.get_selected_columns():
        alias = select_col.expression.alias
        name = select_col.name
        if alias is None or name is None:
            logger.warning(
                "Missing alias or name for selected expression",
                extra={
                    "selected_expression_name": name,
                    "selected_expression_alias": alias,
                },
                exc_info=True,
            )
        elif alias in alias_name_mapping and alias_name_mapping[alias] != name:
            logger.warning(
                "Duplicated alias definition in select clause",
                extra={
                    "alias": alias,
                    "name": name,
                    "existing_name": alias_name_mapping[alias],
                },
                exc_info=True,
            )
        else:
            alias_name_mapping[alias] = name

    transform_column_names(result, alias_name_mapping)

    return result


def _format_storage_query_and_run(
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    referrer: str,
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    request_settings: RequestSettings,
    reader: Reader,
) -> QueryResult:
    """
    Formats the Storage Query and pass it to the DB specific code for execution.
    """
    from_clause = clickhouse_query.get_from_clause()
    visitor = TablesCollector()
    visitor.visit(from_clause)
    table_names = ",".join(sorted(visitor.get_tables()))
    with sentry_sdk.start_span(description="create_query", op="db") as span:
        formatted_query = format_query(clickhouse_query, request_settings)
        span.set_data("query", formatted_query.structured())
        metrics.increment("execute")

    timer.mark("prepare_query")

    stats = {
        "clickhouse_table": table_names,
        "final": visitor.any_final(),
        "referrer": referrer,
        "sample": visitor.get_sample_rate(),
    }

    with sentry_sdk.start_span(description=formatted_query.get_sql(), op="db") as span:
        span.set_tag("table", table_names)

        return raw_query(
            clickhouse_query,
            request_settings,
            formatted_query,
            reader,
            timer,
            query_metadata,
            stats,
            span.trace_id,
        )
