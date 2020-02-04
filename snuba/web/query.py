import copy
import logging

from dataclasses import dataclass
from hashlib import md5
from typing import (
    Any,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
)

import sentry_sdk
from clickhouse_driver.errors import Error as ClickHouseError
from flask import request as http_request
from functools import partial

from snuba import settings, state
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
from snuba.web.split import split_query

logger = logging.getLogger("snuba.query")
metrics = create_metrics("snuba.api")

clickhouse_rw = ClickhousePool()
clickhouse_ro = ClickhousePool(client_settings={"readonly": True})

ClickhouseQueryResult = MutableMapping[str, MutableMapping[str, Any]]


class RawQueryException(Exception):
    def __init__(
        self, err_type: str, message: str, stats: Mapping[str, Any], sql: str, **meta
    ):
        self.err_type = err_type
        self.message = message
        self.stats = stats
        self.sql = sql
        self.meta = meta


@dataclass(frozen=True)
class ClickhouseQueryMetadata:
    sql: str
    timer: Timer
    stats: Mapping[str, Any]
    status: str
    trace_id: str

    def to_dict(self):
        return {
            "sql": self.sql,
            "timing": self.timer.for_json(),
            "stats": self.stats,
            "status": self.status,
            "trace_id": self.trace_id,
        }


cache: Cache[Any] = RedisCache(redis_client, "snuba-query-cache:", JSONCodec())


def raw_query(
    request: Request,
    query: DictClickhouseQuery,
    reader: Reader[ClickhouseQuery],
    timer: Timer,
    query_list: MutableSequence[ClickhouseQueryMetadata],
    stats: Optional[MutableMapping[str, Any]] = None,
    trace_id: str = "",
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
            update_query_list_and_stats,
            request,
            sql,
            timer,
            stats,
            query_list,
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
                        if isinstance(ex, ClickHouseError):
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

    if settings.STATS_IN_RESPONSE or request.settings.get_debug():
        result["stats"] = stats
        result["sql"] = sql

    return result


def update_query_list_and_stats(
    request: Request,
    sql: str,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_list: MutableSequence[ClickhouseQueryMetadata],
    query_settings: Mapping[str, Any],
    trace_id: str,
    status: str,
) -> MutableMapping:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """

    stats.update(query_settings)

    query_list.append(
        ClickhouseQueryMetadata(
            sql=sql,
            timer=timer,
            stats=stats,
            status=status,
            trace_id=trace_id,
        )
    )

    return stats


def record_query(
    request: Request, timer: Timer, query_list: MutableSequence[ClickhouseQueryMetadata]
) -> None:
    if settings.RECORD_QUERIES:
        # send to redis
        state.record_query(
            {
                "timing": timer.for_json(),
                "request": request.body,
                "referrer": http_request.referrer,
                "query_list": [q.to_dict() for q in query_list],
            }
        )

        last_query = query_list[-1]
        final = str(request.query.get_final())
        referrer = request.referrer or "none"
        timer.send_metrics_to(
            metrics,
            tags={
                "status": str(last_query.status),
                "referrer": referrer,
                "final": final,
            },
            mark_tags={"final": final},
        )


def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> ClickhouseQueryResult:
    """
    Runs a query, then records the results including metadata about each split.
    """
    query_list: MutableSequence[ClickhouseQueryMetadata] = []
    request_copy = copy.deepcopy(request)
    try:
        result = _run_query(
            dataset=dataset, request=request, timer=timer, query_list=query_list
        )
        record_query(request_copy, timer, query_list)
    except RawQueryException as error:
        record_query(request_copy, timer, query_list)
        raise error

    return result


@split_query
def _run_query(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    query_list: MutableSequence[ClickhouseQueryMetadata],
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
            request,
            query,
            NativeDriverReader(clickhouse_ro),
            timer,
            query_list,
            stats,
            span.trace_id,
        )

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            if "max_threads" in stats:
                scope.span.set_tag("max_threads", stats["max_threads"])

    return result
