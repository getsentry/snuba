import logging

from hashlib import md5
from typing import Any, Mapping, MutableMapping, Optional

import sentry_sdk
from clickhouse_driver.errors import Error as ClickHouseError
from flask import request as http_request

from snuba import settings, state
from snuba.api.split import split_query
from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.query import ClickhouseQuery, DictClickhouseQuery
from snuba.datasets.dataset import Dataset
from snuba.query.timeseries import TimeSeriesExtensionProcessor
from snuba.reader import Reader
from snuba.request import Request
from snuba.redis import redis_client
from snuba.state.cache import Cache, RedisCache
from snuba.state.rate_limit import (
    RateLimitAggregator,
    RateLimitExceeded,
    PROJECT_RATE_LIMIT_NAME,
)
from snuba.util import create_metrics, force_bytes
from snuba.utils.codecs import JSONCodec
from snuba.utils.metrics.timer import Timer

logger = logging.getLogger("snuba.query")
metrics = create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, "snuba.api")

clickhouse_rw = ClickhousePool()
clickhouse_ro = ClickhousePool(client_settings={"readonly": True})

ClickhouseQueryResult = MutableMapping[str, MutableMapping[str, Any]]


class RawQueryException(Exception):
    def __init__(
        self,
        err_type: str,
        message: str,
        stats: Mapping[str, Any],
        timer: Timer,
        sql: str,
        **meta
    ):
        self.err_type = err_type
        self.message = message
        self.stats = stats
        self.timer = timer
        self.sql = sql
        self.meta = meta


cache: Cache[Any] = RedisCache(redis_client, "snuba-query-cache:", JSONCodec())


def raw_query(
    request: Request,
    query: DictClickhouseQuery,
    reader: Reader[ClickhouseQuery],
    timer: Timer,
    stats: Optional[MutableMapping[str, Any]] = None,
) -> ClickhouseQueryResult:
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """

    stats = stats or {}
    use_cache, use_deduper, uc_max = state.get_configs(
        [("use_cache", 0), ("use_deduper", 1), ("uncompressed_cache_max_cols", 5)]
    )

    all_confs = state.get_all_configs()
    query_settings = {
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

                        logger.debug(sql)
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
                        stats = log_query_and_update_stats(
                            request, sql, timer, stats, "error", query_settings
                        )
                        meta = {}
                        if isinstance(ex, ClickHouseError):
                            err_type = "clickhouse"
                            meta["code"] = ex.code
                        else:
                            err_type = "unknown"
                        raise RawQueryException(
                            err_type=err_type,
                            message=error,
                            stats=stats,
                            timer=timer,
                            sql=sql,
                            **meta,
                        )
            except RateLimitExceeded as ex:
                stats = log_query_and_update_stats(
                    request, sql, timer, stats, "rate-limited", query_settings
                )
                raise RawQueryException(
                    err_type="rate-limited",
                    message="rate limit exceeded",
                    stats=stats,
                    timer=timer,
                    sql=sql,
                    detail=str(ex),
                )

    stats = log_query_and_update_stats(
        request, sql, timer, stats, "success", query_settings
    )

    result["timing"] = timer

    if settings.STATS_IN_RESPONSE or request.settings.get_debug():
        result["stats"] = stats
        result["sql"] = sql

    return result


def log_query_and_update_stats(
    request: Request,
    sql: str,
    timer: Timer,
    stats: MutableMapping[str, Any],
    status: str,
    query_settings: Mapping[str, Any],
) -> MutableMapping:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """

    stats.update(query_settings)
    if settings.RECORD_QUERIES:
        # send to redis
        state.record_query(
            {
                "request": request.body,
                "sql": sql,
                "timing": timer,
                "stats": stats,
                "status": status,
            }
        )

        timer.send_metrics_to(
            metrics,
            tags={
                "status": str(status),
                "referrer": stats.get("referrer", "none"),
                "final": str(stats.get("final", False)),
            },
            mark_tags={"final": str(stats.get("final", False))},
        )
    return stats


@split_query
def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> ClickhouseQueryResult:
    from_date, to_date = TimeSeriesExtensionProcessor.get_time_limit(
        request.extensions["timeseries"]
    )

    extensions = dataset.get_extensions()
    for name, extension in extensions.items():
        extension.get_processor().process_query(
            request.query, request.extensions[name], request.settings
        )

    request.query.add_conditions(dataset.default_conditions())

    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in dataset.get_query_processors():
        processor.process_query(request.query, request.settings)

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
        result = raw_query(
            request, query, NativeDriverReader(clickhouse_ro), timer, stats
        )

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            if "max_threads" in stats:
                scope.span.set_tag("max_threads", stats["max_threads"])

    return result
