from __future__ import annotations

import logging
import textwrap
from dataclasses import replace
from math import floor
from typing import Any, MutableMapping, Optional

import sentry_sdk

from snuba import environment
from snuba import settings as snuba_settings
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineStage
from snuba.pipeline.utils.storage_finder import StorageKeyFinder
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import (
    QueryStatus,
    SnubaQueryMetadata,
    get_request_status,
)
from snuba.reader import Reader
from snuba.settings import MAX_QUERY_SIZE_BYTES
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import (
    QueryException,
    QueryExtraData,
    QueryResult,
    QueryTooLongException,
    transform_column_names,
)
from snuba.web.db_query import db_query, update_query_metadata_and_stats

metrics = MetricsWrapper(environment.metrics, "api")
logger = logging.getLogger("snuba.pipeline.stages.query_execution")


class ExecutionStage(
    QueryPipelineStage[ClickhouseQuery | CompositeQuery[Table], QueryResult]
):
    def __init__(
        self,
        attribution_info: AttributionInfo,
        query_metadata: SnubaQueryMetadata,
        robust: bool = False,
        concurrent_queries_gauge: Optional[Gauge] = None,
    ):
        self._attribution_info = attribution_info
        self._query_metadata = query_metadata
        # NOTE: (Volo) this should probably be a query setting
        self._robust = robust
        # NOTE (Volo): this should probably exist by default or not at all
        self._concurrent_queries_gauge = concurrent_queries_gauge

    def get_cluster(
        self, query: ClickhouseQuery | CompositeQuery[Table], settings: QuerySettings
    ) -> ClickhouseCluster:
        storage_key = StorageKeyFinder().visit(query)
        storage = get_storage(storage_key)
        if is_storage_set_sliced(storage.get_storage_set_key()):
            raise NotImplementedError("sliced storages not supported in new pipeline")
        return storage.get_cluster()

    def _process_data(
        self, pipe_input: QueryPipelineData[ClickhouseQuery | CompositeQuery[Table]]
    ) -> QueryResult:
        cluster = self.get_cluster(pipe_input.data, pipe_input.query_settings)
        if pipe_input.query_settings.get_dry_run():
            return _dry_run_query_runner(
                clickhouse_query=pipe_input.data,
                cluster_name=getattr(
                    cluster, "get_clickhouse_cluster_name", lambda: "no_cluster_name"
                )(),
            )
        else:
            return _run_and_apply_column_names(
                timer=pipe_input.timer,
                query_metadata=self._query_metadata,
                attribution_info=self._attribution_info,
                robust=self._robust,
                concurrent_queries_gauge=None,
                clickhouse_query=pipe_input.data,
                query_settings=pipe_input.query_settings,
                reader=cluster.get_reader(),
                cluster_name=cluster.get_clickhouse_cluster_name() or "",
            )


def _dry_run_query_runner(
    clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
    cluster_name: str,
) -> QueryResult:
    with sentry_sdk.start_span(
        description="dryrun_create_query", op="function"
    ) as span:
        formatted_query = format_query(clickhouse_query)
        span.set_data("query", formatted_query.structured())

    return QueryResult(
        {"data": [], "meta": []},
        {
            "stats": {"cluster_name": cluster_name},
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
    clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
    query_settings: QuerySettings,
    reader: Reader,
    cluster_name: str,
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
        cluster_name,
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
    clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
    query_settings: QuerySettings,
    reader: Reader,
    robust: bool,
    concurrent_queries_gauge: Optional[Gauge] = None,
    cluster_name: str = "",
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

    stats: MutableMapping[str, Any] = {
        "clickhouse_table": table_names,
        "final": visitor.any_final(),
        "referrer": attribution_info.referrer,
        "sample": visitor.get_sample_rate(),
        "cluster_name": cluster_name,
    }

    if query_size_bytes > MAX_QUERY_SIZE_BYTES:
        cause = QueryTooLongException(
            f"After processing, query is {query_size_bytes} bytes, "
            "which is too long for ClickHouse to process. "
            f"Max size is {MAX_QUERY_SIZE_BYTES} bytes."
        )
        stats = update_query_metadata_and_stats(
            query=clickhouse_query,
            sql=formatted_query.get_sql(),
            stats=stats,
            query_metadata_list=query_metadata.query_list,
            query_settings={},
            trace_id="",
            status=QueryStatus.INVALID_REQUEST,
            request_status=get_request_status(cause),
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
    clickhouse_query: ClickhouseQuery | CompositeQuery[Table],
    query_settings: QuerySettings,
) -> None:
    """
    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """
    if isinstance(clickhouse_query, ClickhouseQuery):
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
