from __future__ import annotations

import logging
import textwrap
from dataclasses import replace
from functools import partial
from math import floor
from typing import MutableMapping, Optional, Set, Union

import sentry_sdk

from snuba import environment
from snuba import settings as snuba_settings
from snuba.attribution.attribution_info import AttributionInfo
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
from snuba.query.exceptions import QueryPlanException
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import QuerySettings
from snuba.querylog import record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.reader import Reader
from snuba.request import Request
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import (
    QueryException,
    QueryExtraData,
    QueryResult,
    QueryTooLongException,
    transform_column_names,
)
from snuba.web.db_query import db_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")

MAX_QUERY_SIZE_BYTES = 256 * 1024  # 256 KiB by default


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
    entity_name = "unknown"
    if isinstance(request.query, LogicalQuery):
        entity_key = request.query.get_from_clause().key
        entity = get_entity(entity_key)
        entity_name = entity_key.value
        if entity.required_time_column is not None:
            start, end = get_time_range(request.query, entity.required_time_column)

    query_metadata = SnubaQueryMetadata(
        request=request,
        start_timestamp=start,
        end_timestamp=end,
        dataset=get_dataset_name(dataset),
        entity=entity_name,
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
        if not request.query_settings.get_dry_run():
            record_query(request, timer, query_metadata, result)
    except QueryException as error:
        _set_query_final(request, error.extra)
        record_query(request, timer, query_metadata, error)
        raise error
    except QueryPlanException as error:
        record_query(request, timer, query_metadata, error)
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


    ** GOTCHAS **

    Something which is not immediately clear from looking at the code is that the
    query_runner can be run multiple times during the execution of the pipeline.
    The execution pipeline may choose to break up a query into multiple subqueries. And
    then assemble those together into one resut

    Throughout those executions, the query_metadata.query_list is appended to every time a query runs
    within `db_query.py` with metadata about the query. That metadata then goes into the querylog.

    There is the possibility that the `query_runner` is used across different threads. In that case,
    there *may* be a race condition on the `query_list`. At time of writing (27-03-2023) this is not a concern because:

      - MultipleConcurrentPipeline is not in use and therefore this does not happen in practice
      - Even when the runner function is invoked across multiple threads, threads in python are not truly paralllel
      - synchornizing locks for mostly theoretical analytics reasons does not seem worth it. When you are reading
          this comment, that may no longer be true

    """
    if request.query_settings.get_dry_run():
        query_runner = _dry_run_query_runner
    else:
        query_runner = partial(
            _run_and_apply_column_names,
            timer=timer,
            query_metadata=query_metadata,
            attribution_info=request.attribution_info,
            robust=robust,
            concurrent_queries_gauge=concurrent_queries_gauge,
        )

    record_missing_use_case_id(request, dataset)

    return (
        dataset.get_query_pipeline_builder()
        .build_execution_pipeline(request, query_runner)
        .execute()
    )


def record_missing_use_case_id(request: Request, dataset: Dataset) -> None:
    """
    Used to track how often the new `use_case_id` Tenant ID is not included in
    a Generic Metrics request.
    """
    if get_dataset_name(dataset) == "generic_metrics":
        if (
            not (tenant_ids := request.attribution_info.tenant_ids)
            or (use_case_id := tenant_ids.get("use_case_id")) is None
        ):
            metrics.increment(
                "gen_metrics_request_without_use_case_id",
                tags={"referrer": request.referrer},
            )
        else:
            metrics.increment(
                "gen_metrics_request_with_use_case_id",
                tags={
                    "referrer": request.referrer,
                    "use_case_id": str(use_case_id),
                },
            )


def _dry_run_query_runner(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    reader: Reader,
) -> QueryResult:
    # NOTE (Volo) : this is misleading behavior. If this runner is used with a split query,
    # you will only see the sql reported that the first of the split queries ran. Since this returns
    # no results, you won't see any others

    with sentry_sdk.start_span(
        description="dryrun_create_query", op="function"
    ) as span:
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
    attribution_info: AttributionInfo,
    robust: bool,
    concurrent_queries_gauge: Optional[Gauge],
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
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
        attribution_info,
        clickhouse_query,
        query_settings,
        reader,
        robust,
        concurrent_queries_gauge,
    )

    alias_name_mapping: MutableMapping[str, list[str]] = {}
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
        elif alias in alias_name_mapping and name not in alias_name_mapping[alias]:
            alias_name_mapping[alias].append(name)
        else:
            alias_name_mapping[alias] = [name]

    transform_column_names(result, alias_name_mapping)
    return result


def _format_storage_query_and_run(
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    attribution_info: AttributionInfo,
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
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
    with sentry_sdk.start_span(description="create_query", op="function") as span:
        _apply_turbo_sampling_if_needed(clickhouse_query, query_settings)

        formatted_query = format_query(clickhouse_query)

        formatted_sql = formatted_query.get_sql()
        query_size_bytes = len(formatted_sql.encode("utf-8"))
        span.set_data(
            "query", textwrap.wrap(formatted_sql, 100, break_long_words=False)
        )  # To avoid the query being truncated
        span.set_data("table", table_names)
        span.set_data("query_size_bytes", query_size_bytes)
        sentry_sdk.set_tag("query_size_group", get_query_size_group(query_size_bytes))
        metrics.increment(
            "execute",
            tags={
                "table": table_names,
                "referrer": attribution_info.referrer,
                "dataset": query_metadata.dataset,
            },
        )

    timer.mark("prepare_query")

    stats = {
        "clickhouse_table": table_names,
        "final": visitor.any_final(),
        "referrer": attribution_info.referrer,
        "sample": visitor.get_sample_rate(),
    }

    if query_size_bytes > MAX_QUERY_SIZE_BYTES:
        cause = QueryTooLongException(
            f"After processing, query is {query_size_bytes} bytes, "
            "which is too long for ClickHouse to process. "
            f"Max size is {MAX_QUERY_SIZE_BYTES} bytes."
        )

        raise QueryException.from_args(
            cause.__class__.__name__,
            str(cause),
            extra=QueryExtraData(
                stats=stats,
                sql=formatted_sql,
                experiments=clickhouse_query.get_experiments(),
            ),
        ) from cause
    with sentry_sdk.start_span(description=formatted_sql, op="function") as span:
        span.set_tag("table", table_names)

        def execute() -> QueryResult:
            return db_query(
                clickhouse_query=clickhouse_query,
                query_settings=query_settings,
                attribution_info=attribution_info,
                dataset_name=query_metadata.dataset,
                formatted_query=formatted_query,
                reader=reader,
                timer=timer,
                query_metadata_list=query_metadata.query_list,
                stats=stats,
                trace_id=span.trace_id,
                robust=robust,
            )

        if concurrent_queries_gauge is not None:
            with concurrent_queries_gauge:
                return execute()
        else:
            return execute()


def get_query_size_group(query_size_bytes: int) -> str:
    """
    Given the size of a query string in bytes, returns a string
    representing the size of the query in 10% grouped increments
    of the Maximum Query Size as defined in Snuba settings.

    Eg. If the query size is 40-49% of the max query size, this function
    returns ">=40%".

    Eg. If the query size is equal to the max query size, this function
    returns "100%".
    """
    if query_size_bytes == MAX_QUERY_SIZE_BYTES:
        return "100%"
    else:
        query_size_group = int(floor(query_size_bytes / MAX_QUERY_SIZE_BYTES * 10)) * 10
        return f">={query_size_group}%"


def _apply_turbo_sampling_if_needed(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
) -> None:
    """
    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """
    if isinstance(clickhouse_query, Query):
        if (
            query_settings.get_turbo()
            and not clickhouse_query.get_from_clause().sampling_rate
        ):
            clickhouse_query.set_from_clause(
                replace(
                    clickhouse_query.get_from_clause(),
                    sampling_rate=snuba_settings.TURBO_SAMPLE_RATE,
                )
            )
