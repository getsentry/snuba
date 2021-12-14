import logging
from dataclasses import replace
from functools import partial
from math import floor
from typing import MutableMapping, Optional, Set, Union

import sentry_sdk

from snuba import environment
from snuba import settings as snuba_settings
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import (
    get_object_ids_in_query_ast,
    get_time_range,
)
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.logical import Query as LogicalQuery
from snuba.querylog import record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.util import with_span
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import (
    QueryException,
    QueryExtraData,
    QueryResult,
    transform_column_names,
)
from snuba.web.db_query import raw_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")

MAX_QUERY_SIZE_BYTES = 256 * 1024  # 256 KiB by default


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


class ProjectsFinder(
    DataSourceVisitor[Set[int], Entity], JoinVisitor[Set[int], Entity]
):
    """
    Traverses a query to find project_id conditions
    """

    def _visit_simple_source(self, data_source: Entity) -> Set[int]:
        return set()

    def _visit_join(self, data_source: JoinClause[Entity]) -> Set[int]:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Entity]) -> Set[int]:
        return get_object_ids_in_query_ast(data_source, "project_id") or set()

    def _visit_composite_query(self, data_source: CompositeQuery[Entity]) -> Set[int]:
        return self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[Entity]) -> Set[int]:
        return self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Entity]) -> Set[int]:
        left = node.left_node.accept(self)
        right = node.right_node.accept(self)
        return left | right


@with_span()
def parse_and_run_query(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    robust: bool = False,
    concurrent_queries_gauge: Optional[Gauge] = None,
) -> QueryResult:
    """
    Runs a Snuba Query, then records the metadata about each split query that was run.
    """
    # from_clause = request.query.get_from_clause()
    start, end = None, None
    if isinstance(request.query, LogicalQuery):
        entity = get_entity(request.query.get_from_clause().key)
        if entity.required_time_column is not None:
            start, end = get_time_range(request.query, entity.required_time_column)

    query_metadata = SnubaQueryMetadata(
        request=request,
        start_timestamp=start,
        end_timestamp=end,
        dataset=get_dataset_name(dataset),
        timer=timer,
        query_list=[],
        projects=ProjectsFinder().visit(request.query),
        snql_anonymized=request.snql_anonymized,
    )

    try:
        result = _run_query_pipeline(
            dataset=dataset,
            request=request,
            timer=timer,
            query_metadata=query_metadata,
            robust=robust,
            concurrent_queries_gauge=concurrent_queries_gauge,
        )
        _set_query_final(request, result.extra)
        if not request.settings.get_dry_run():
            record_query(request, timer, query_metadata, result.extra)
    except QueryException as error:
        _set_query_final(request, error.extra)
        record_query(request, timer, query_metadata, error.extra)
        raise error

    return result


def _set_query_final(request: Request, extra: QueryExtraData) -> None:
    if "final" in extra["stats"]:
        request.query.set_final(extra["stats"]["final"])


def _run_query_pipeline(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    robust: bool,
    concurrent_queries_gauge: Optional[Gauge],
) -> QueryResult:
    """
    Runs the query processing and execution pipeline for a Snuba Query. This means it takes a Dataset
    and a Request and returns the results of the query.

    This process includes:
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

    if request.settings.get_dry_run():
        query_runner = _dry_run_query_runner
    else:
        query_runner = partial(
            _run_and_apply_column_names,
            timer,
            query_metadata,
            request.referrer,
            robust,
            concurrent_queries_gauge,
        )

    return (
        dataset.get_query_pipeline_builder()
        .build_execution_pipeline(request, query_runner)
        .execute()
    )


def _dry_run_query_runner(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    request_settings: RequestSettings,
    reader: Reader,
) -> QueryResult:
    with sentry_sdk.start_span(description="dryrun_create_query", op="db") as span:
        formatted_query = format_query(clickhouse_query)
        span.set_data("query", formatted_query.structured())

    return QueryResult(
        {"data": [], "meta": []},
        {
            "stats": {},
            "sql": formatted_query.get_sql(),
            "experiments": clickhouse_query.get_experiments(),
        },
    )


def _run_and_apply_column_names(
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    referrer: str,
    robust: bool,
    concurrent_queries_gauge: Optional[Gauge],
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
        timer,
        query_metadata,
        referrer,
        clickhouse_query,
        request_settings,
        reader,
        robust,
        concurrent_queries_gauge,
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
                    "column_name": name,
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
    robust: bool,
    concurrent_queries_gauge: Optional[Gauge] = None,
) -> QueryResult:
    """
    Formats the Storage Query and pass it to the DB specific code for execution.
    """
    from_clause = clickhouse_query.get_from_clause()
    visitor = TablesCollector()
    visitor.visit(from_clause)
    table_names = ",".join(sorted(visitor.get_tables()))
    with sentry_sdk.start_span(description="create_query", op="db") as span:
        _apply_turbo_sampling_if_needed(clickhouse_query, request_settings)

        formatted_query = format_query(clickhouse_query)
        span.set_data("query", formatted_query.structured())
        span.set_data(
            "query_size_bytes", _string_size_in_bytes(formatted_query.get_sql())
        )
        sentry_sdk.set_tag(
            "query_size_group", get_query_size_group(formatted_query.get_sql())
        )
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

        def execute() -> QueryResult:
            return raw_query(
                clickhouse_query,
                request_settings,
                formatted_query,
                reader,
                timer,
                query_metadata,
                stats,
                span.trace_id,
                robust=robust,
            )

        if concurrent_queries_gauge is not None:
            with concurrent_queries_gauge:
                return execute()
        else:
            return execute()


def get_query_size_group(formatted_query: str) -> str:
    """
    Given a formatted query string (or any string technically),
    returns a string representing the size of the query in 10%
    grouped increments of the Maximum Query Size as defined in Snuba settings.

    Eg. If the query size is 40-49% of the max query size, this function
    returns ">=40%".

    All sizes are computed in Bytes.
    """
    query_size_in_bytes = _string_size_in_bytes(formatted_query)
    if query_size_in_bytes >= MAX_QUERY_SIZE_BYTES:
        query_size_group = 100
    else:
        query_size_group = (
            int(floor(query_size_in_bytes / MAX_QUERY_SIZE_BYTES * 10)) * 10
        )
    return f">={query_size_group}%"


def _string_size_in_bytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _apply_turbo_sampling_if_needed(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    request_settings: RequestSettings,
) -> None:
    """
    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """
    if isinstance(clickhouse_query, Query):
        if (
            request_settings.get_turbo()
            and not clickhouse_query.get_from_clause().sampling_rate
        ):
            clickhouse_query.set_from_clause(
                replace(
                    clickhouse_query.get_from_clause(),
                    sampling_rate=snuba_settings.TURBO_SAMPLE_RATE,
                )
            )
