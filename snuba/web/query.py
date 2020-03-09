import copy
import logging

from datetime import datetime
from hashlib import md5
from typing import (
    Any,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
)

import sentry_sdk
from flask import request as http_request
from functools import partial

from snuba import environment, settings, state
from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.query import DictClickhouseQuery
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.datasets.plans.query_plan import RawQueryResult
from snuba.environment import reader
from snuba.query.query import Query
from snuba.query.timeseries import TimeSeriesExtensionProcessor
from snuba.redis import redis_client
from snuba.request import Request, RequestSettings
from snuba.state.cache import Cache, RedisCache
from snuba.state.rate_limit import (
    PROJECT_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
)
from snuba.util import force_bytes
from snuba.utils.codecs import JSONCodec
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.utils.metrics.timer import Timer
from snuba.web.query_metadata import ClickhouseQueryMetadata, SnubaQueryMetadata
from snuba.web.split import split_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")


class RawQueryException(Exception):
    def __init__(
        self, err_type: str, message: str, stats: Mapping[str, Any], sql: str, **meta
    ):
        self.err_type = err_type
        self.message = message
        self.stats = stats
        self.sql = sql
        self.meta = meta


cache: Cache[Any] = RedisCache(redis_client, "snuba-query-cache:", JSONCodec())


def raw_query(
    request: Request,
    query: DictClickhouseQuery,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    stats: MutableMapping[str, Any],
    trace_id: Optional[str] = None,
) -> RawQueryResult:
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """

    use_cache, use_deduper, uc_max = state.get_configs(
        [
            ("use_cache", settings.USE_RESULT_CACHE),
            ("use_deduper", 1),
            ("uncompressed_cache_max_cols", 5),
        ]
    )

    all_confs = state.get_all_configs()
    query_settings: MutableMapping[str, Any] = {
        k.split("/", 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith("query_settings/")
    }

    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in clickhouse, or result cache in snuba.
    if len(request.query.get_all_referenced_columns()) > uc_max:
        query_settings["use_uncompressed_cache"] = 0
        use_cache = 0

    timer.mark("get_configs")

    sql = query.format_sql()
    query_id = md5(force_bytes(sql)).hexdigest()
    with state.deduper(query_id if use_deduper else None) as is_dupe:
        timer.mark("dedupe_wait")

        result = cache.get(query_id) if use_cache else None
        timer.mark("cache_get")

        stats.update(
            {
                "is_duplicate": is_dupe,
                "query_id": query_id,
                "use_cache": bool(use_cache),
                "cache_hit": bool(result),
            }
        ),

        update_with_status = partial(
            update_query_metadata_and_stats,
            request,
            sql,
            timer,
            stats,
            query_metadata,
            query_settings,
            trace_id,
        )

        if not result:
            try:
                with RateLimitAggregator(
                    request.settings.get_rate_limit_params()
                ) as rate_limit_stats_container:
                    stats.update(rate_limit_stats_container.to_dict())
                    timer.mark("rate_limit")

                    project_rate_limit_stats = rate_limit_stats_container.get_stats(
                        PROJECT_RATE_LIMIT_NAME
                    )

                    if (
                        "max_threads" in query_settings
                        and project_rate_limit_stats is not None
                        and project_rate_limit_stats.concurrent > 1
                    ):
                        maxt = query_settings["max_threads"]
                        query_settings["max_threads"] = max(
                            1, maxt - project_rate_limit_stats.concurrent + 1
                        )

                    # Force query to use the first shard replica, which
                    # should have synchronously received any cluster writes
                    # before this query is run.
                    consistent = request.settings.get_consistent()
                    stats["consistent"] = consistent
                    if consistent:
                        query_settings["load_balancing"] = "in_order"
                        query_settings["max_threads"] = 1

                    try:
                        result = reader.execute(
                            query,
                            query_settings,
                            # All queries should already be deduplicated at this point
                            # But the query_id will let us know if they aren't
                            query_id=query_id if use_deduper else None,
                            with_totals=request.query.has_totals(),
                        )

                        timer.mark("execute")
                        stats.update(
                            {
                                "result_rows": len(result["data"]),
                                "result_cols": len(result["meta"]),
                            }
                        )

                        if use_cache:
                            cache.set(query_id, result)
                            timer.mark("cache_set")

                    except BaseException as ex:
                        error = str(ex)
                        logger.exception("Error running query: %s\n%s", sql, error)
                        stats = update_with_status("error")
                        meta = {}
                        if isinstance(ex, ClickhouseError):
                            err_type = "clickhouse"
                            meta["code"] = ex.code
                        else:
                            err_type = "unknown"
                        raise RawQueryException(
                            err_type=err_type,
                            message=error,
                            stats=stats,
                            sql=sql,
                            **meta,
                        )
            except RateLimitExceeded as ex:
                stats = update_with_status("rate-limited")
                raise RawQueryException(
                    err_type="rate-limited",
                    message="rate limit exceeded",
                    stats=stats,
                    sql=sql,
                    detail=str(ex),
                )

    stats = update_with_status("success")

    return RawQueryResult(result, {"stats": stats, "sql": sql})


def update_query_metadata_and_stats(
    request: Request,
    sql: str,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_metadata: SnubaQueryMetadata,
    query_settings: Mapping[str, Any],
    trace_id: Optional[str],
    status: str,
) -> MutableMapping:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """

    stats.update(query_settings)

    query_metadata.query_list.append(
        ClickhouseQueryMetadata(sql=sql, stats=stats, status=status, trace_id=trace_id)
    )

    return stats


def record_query(
    request: Request, timer: Timer, query_metadata: SnubaQueryMetadata
) -> None:
    if settings.RECORD_QUERIES:
        # Send to redis
        # We convert this to a dict before passing it to state in order to avoid a
        # circular dependency, where state would depend on the higher level
        # QueryMetadata class
        state.record_query(query_metadata.to_dict())

        final = str(request.query.get_final())
        referrer = request.referrer or "none"
        timer.send_metrics_to(
            metrics,
            tags={
                "status": query_metadata.status,
                "referrer": referrer,
                "final": final,
            },
            mark_tags={"final": final},
        )


def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> RawQueryResult:
    """
    Runs a query, then records the metadata about each split query that was run.
    """
    request_copy = copy.deepcopy(request)
    query_metadata = SnubaQueryMetadata(
        request=request_copy,
        dataset=get_dataset_name(dataset),
        timer=timer,
        query_list=[],
        referrer=request.referrer,
    )

    try:
        result = _run_query(
            dataset=dataset, request=request, timer=timer, query_metadata=query_metadata
        )
        record_query(request_copy, timer, query_metadata)
    except RawQueryException as error:
        record_query(request_copy, timer, query_metadata)
        raise error

    return result


def _run_clickhouse_query(
    dataset: Dataset,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    from_date: datetime,
    to_date: datetime,
    request: Request,
) -> RawQueryResult:
    # TODO: This below should be a query processor.
    relational_source = request.query.get_data_source()
    request.query.add_conditions(relational_source.get_mandatory_conditions())

    source = relational_source.format_from()
    with sentry_sdk.start_span(description="create_query", op="db"):
        # TODO: consider moving the performance logic and the pre_where generation into
        # ClickhouseQuery since they are Clickhouse specific
        query = DictClickhouseQuery(dataset, request.query, request.settings)
    timer.mark("prepare_query")

    num_days = (to_date - from_date).days
    stats = {
        "clickhouse_table": source,
        "final": request.query.get_final(),
        "referrer": request.referrer,
        "num_days": num_days,
        "sample": request.query.get_sample(),
    }

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            scope.span.set_tag("dataset", type(dataset).__name__)
            scope.span.set_tag("referrer", http_request.referrer)
            scope.span.set_tag("timeframe_days", num_days)

    with sentry_sdk.start_span(description=query.format_sql(), op="db") as span:
        span.set_tag("dataset", type(dataset).__name__)
        span.set_tag("table", source)
        try:
            span.set_tag(
                "ast_query",
                AstClickhouseQuery(request.query, request.settings).format_sql(),
            )
        except Exception:
            logger.exception("Failed to format ast query")

        result = raw_query(request, query, timer, query_metadata, stats, span.trace_id)

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            if "max_threads" in stats:
                scope.span.set_tag("max_threads", stats["max_threads"])

    return result


@split_query
def _run_query(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
) -> RawQueryResult:
    from_date, to_date = TimeSeriesExtensionProcessor.get_time_limit(
        request.extensions["timeseries"]
    )

    if (
        request.query.get_sample() is not None and request.query.get_sample() != 1.0
    ) and not request.settings.get_turbo():
        metrics.increment("sample_without_turbo", tags={"referrer": request.referrer})

    extensions = dataset.get_extensions()
    for name, extension in extensions.items():
        extension.get_processor().process_query(
            request.query, request.extensions[name], request.settings
        )

    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in dataset.get_query_processors():
        processor.process_query(request.query, request.settings)

    storage_query_plan = dataset.get_query_plan_builder().build_plan(request)

    for processor in storage_query_plan.query_processors:
        processor.process_query(request.query, request.settings)

    query_runner = partial(
        _run_clickhouse_query, dataset, timer, query_metadata, from_date, to_date,
    )

    return storage_query_plan.execution_strategy.execute(request, query_runner)
