from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hashlib import md5
from threading import Lock
from typing import Any, Mapping, MutableMapping, Optional, Union, cast

import rapidjson
import sentry_sdk
from clickhouse_driver import errors
from sentry_sdk import Hub
from sentry_sdk.api import configure_scope

from snuba import environment, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.formatter.query import format_query_anonymized
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range_estimate
from snuba.clickhouse.query_profiler import generate_profile
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    QueryStatus,
    SnubaQueryMetadata,
)
from snuba.reader import Reader, Result
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.cache.abstract import Cache, ExecutionTimeoutError
from snuba.state.cache.redis.backend import RESULT_VALUE, RESULT_WAIT, RedisCache
from snuba.state.rate_limit import (
    ORGANIZATION_RATE_LIMIT_NAME,
    PROJECT_RATE_LIMIT_NAME,
    TABLE_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitStats,
    RateLimitStatsContainer,
)
from snuba.util import force_bytes, with_span
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import (
    SerializableException,
    SerializableExceptionDict,
)
from snuba.web import QueryException, QueryResult, constants

metrics = MetricsWrapper(environment.metrics, "db_query")

redis_cache_client = get_redis_client(RedisClientKey.CACHE)


class ResultCacheCodec(ExceptionAwareCodec[bytes, Result]):
    def encode(self, value: Result) -> bytes:
        return cast(str, rapidjson.dumps(value, default=str)).encode("utf-8")

    def decode(self, value: bytes) -> Result:
        ret = rapidjson.loads(value)
        if ret.get("__type__", "DNE") == "SerializableException":
            raise SerializableException.from_dict(cast(SerializableExceptionDict, ret))
        if not isinstance(ret, Mapping) or "meta" not in ret or "data" not in ret:
            raise ValueError("Invalid value type in result cache")
        return cast(Result, ret)

    def encode_exception(self, value: SerializableException) -> bytes:
        return cast(str, rapidjson.dumps(value.to_dict())).encode("utf-8")


DEFAULT_CACHE_PARTITION_ID = "default"

# We are not initializing all the cache partitions here and instead relying on lazy
# initialization because this module only learn of cache partitions ids from the
# reader when running a query.
cache_partitions: MutableMapping[str, Cache[Result]] = {
    DEFAULT_CACHE_PARTITION_ID: RedisCache(
        redis_cache_client,
        "snuba-query-cache:",
        ResultCacheCodec(),
        ThreadPoolExecutor(),
    )
}
# This lock prevents us from initializing the cache twice. The cache is initialized
# with a thread pool. In case of race condition we could create the threads twice which
# is a waste.
cache_partitions_lock = Lock()

logger = logging.getLogger("snuba.query")


def update_query_metadata_and_stats(
    query: Query,
    sql: str,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_metadata: SnubaQueryMetadata,
    query_settings: Mapping[str, Any],
    trace_id: Optional[str],
    status: QueryStatus,
    profile_data: Optional[Mapping[str, Any]] = None,
    error_code: Optional[int] = None,
    triggered_rate_limiter: Optional[str] = None,
) -> MutableMapping[str, Any]:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """
    stats.update(query_settings)
    if error_code is not None:
        stats["error_code"] = error_code
    if triggered_rate_limiter is not None:
        stats["triggered_rate_limiter"] = triggered_rate_limiter
    sql_anonymized = format_query_anonymized(query).get_sql()
    start, end = get_time_range_estimate(query)

    query_metadata.query_list.append(
        ClickhouseQueryMetadata(
            sql=sql,
            sql_anonymized=sql_anonymized,
            start_timestamp=start,
            end_timestamp=end,
            stats=stats,
            status=status,
            profile=generate_profile(query),
            trace_id=trace_id,
            result_profile=profile_data,
        )
    )

    return stats


@with_span(op="db")
def execute_query(
    # TODO: Passing the whole clickhouse query here is needed as long
    # as the execute method depends on it. Otherwise we can make this
    # file rely either entirely on clickhouse query or entirely on
    # the formatter.
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
) -> Result:
    """
    Execute a query and return a result.
    """
    # Force query to use the first shard replica, which
    # should have synchronously received any cluster writes
    # before this query is run.
    consistent = query_settings.get_consistent()
    stats["consistent"] = consistent
    if consistent:
        clickhouse_query_settings["load_balancing"] = "in_order"
        clickhouse_query_settings["max_threads"] = 1

    result = reader.execute(
        formatted_query,
        clickhouse_query_settings,
        with_totals=clickhouse_query.has_totals(),
        robust=robust,
    )

    timer.mark("execute")
    stats.update(
        {"result_rows": len(result["data"]), "result_cols": len(result["meta"])}
    )

    return result


def _record_rate_limit_metrics(
    rate_limit_stats_container: RateLimitStatsContainer,
    reader: Reader,
    stats: MutableMapping[str, Any],
) -> None:
    # This is a temporary metric that will be removed once the organization
    # rate limit has been tuned.
    org_rate_limit_stats = rate_limit_stats_container.get_stats(
        ORGANIZATION_RATE_LIMIT_NAME
    )
    if org_rate_limit_stats is not None:
        metrics.gauge(
            name="org_concurrent",
            value=org_rate_limit_stats.concurrent,
        )
        metrics.gauge(
            name="org_per_second",
            value=org_rate_limit_stats.rate,
        )
    table_rate_limit_stats = rate_limit_stats_container.get_stats(TABLE_RATE_LIMIT_NAME)
    if table_rate_limit_stats is not None:
        metrics.gauge(
            name="table_concurrent",
            value=table_rate_limit_stats.concurrent,
            tags={
                "table": stats.get("clickhouse_table", ""),
                "cache_partition": reader.cache_partition_id
                if reader.cache_partition_id
                else "default",
            },
        )
        metrics.gauge(
            name="table_per_second",
            value=table_rate_limit_stats.rate,
            tags={
                "table": stats.get("clickhouse_table", ""),
                "cache_partition": reader.cache_partition_id
                if reader.cache_partition_id
                else "default",
            },
        )
        metrics.timing(
            name="table_concurrent_v2",
            value=table_rate_limit_stats.concurrent,
            tags={
                "table": stats.get("clickhouse_table", ""),
                "cache_partition": reader.cache_partition_id
                if reader.cache_partition_id
                else "default",
            },
        )


def _apply_thread_quota_to_clickhouse_query_settings(
    query_settings: QuerySettings,
    clickhouse_query_settings: MutableMapping[str, Any],
    project_rate_limit_stats: Optional[RateLimitStats],
) -> None:
    thread_quota = query_settings.get_resource_quota()
    if (
        "max_threads" in clickhouse_query_settings or thread_quota is not None
    ) and project_rate_limit_stats is not None:
        maxt = (
            clickhouse_query_settings["max_threads"]
            if thread_quota is None
            else thread_quota.max_threads
        )
        clickhouse_query_settings["max_threads"] = max(
            1, maxt - project_rate_limit_stats.concurrent + 1
        )


@with_span(op="db")
def execute_query_with_rate_limits(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
) -> Result:
    # XXX: We should consider moving this that it applies to the logical query,
    # not the physical query.
    with RateLimitAggregator(
        query_settings.get_rate_limit_params()
    ) as rate_limit_stats_container:
        stats.update(rate_limit_stats_container.to_dict())
        timer.mark("rate_limit")

        project_rate_limit_stats = rate_limit_stats_container.get_stats(
            PROJECT_RATE_LIMIT_NAME
        )
        _apply_thread_quota_to_clickhouse_query_settings(
            query_settings, clickhouse_query_settings, project_rate_limit_stats
        )

        _record_rate_limit_metrics(rate_limit_stats_container, reader, stats)

        return execute_query(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust=robust,
        )


def get_query_cache_key(formatted_query: FormattedQuery) -> str:
    return md5(force_bytes(formatted_query.get_sql())).hexdigest()


def _get_cache_partition(reader: Reader) -> Cache[Result]:
    enable_cache_partitioning = state.get_config("enable_cache_partitioning", 1)
    if not enable_cache_partitioning:
        return cache_partitions[DEFAULT_CACHE_PARTITION_ID]

    partition_id = reader.cache_partition_id
    if partition_id is not None and partition_id not in cache_partitions:
        with cache_partitions_lock:
            # This condition was checked before as this lock should be acquired only
            # during the first query. So, for the vast majority of queries, the overhead
            # of acquiring the lock is not needed.
            if partition_id not in cache_partitions:
                cache_partitions[partition_id] = RedisCache(
                    redis_cache_client,
                    f"snuba-query-cache:{partition_id}:",
                    ResultCacheCodec(),
                    ThreadPoolExecutor(),
                )

    return cache_partitions[
        partition_id if partition_id is not None else DEFAULT_CACHE_PARTITION_ID
    ]


@with_span(op="db")
def execute_query_with_query_id(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
) -> Result:
    query_id = get_query_cache_key(formatted_query)

    try:
        return execute_query_with_readthrough_caching(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust,
            query_id,
        )
    except ClickhouseError as e:
        if (
            e.code != errors.ErrorCodes.QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING
            or not state.get_config("retry_duplicate_query_id", False)
        ):
            raise

        logger.error(
            "Query cache for query ID %s lost, retrying query with random ID", query_id
        )
        metrics.increment("query_cache_lost")

        query_id = f"randomized-{uuid.uuid4().hex}"

        return execute_query_with_readthrough_caching(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust,
            query_id,
        )


@with_span(op="db")
def execute_query_with_readthrough_caching(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
    query_id: str,
) -> Result:
    clickhouse_query_settings["query_id"] = query_id

    span = Hub.current.scope.span
    if span:
        span.set_data("query_id", query_id)

    def record_cache_hit_type(hit_type: int) -> None:
        span_tag = "cache_miss"
        if hit_type == RESULT_VALUE:
            stats["cache_hit"] = 1
            span_tag = "cache_hit"
        elif hit_type == RESULT_WAIT:
            stats["is_duplicate"] = 1
            span_tag = "cache_wait"
        sentry_sdk.set_tag("cache_status", span_tag)
        if span:
            span.set_data("cache_status", span_tag)

    cache_partition = _get_cache_partition(reader)
    metrics.increment(
        "cache_partition_loaded",
        tags={"partition_id": reader.cache_partition_id or "default"},
    )

    return cache_partition.get_readthrough(
        query_id,
        partial(
            execute_query_with_rate_limits,
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust,
        ),
        record_cache_hit_type=record_cache_hit_type,
        timeout=_get_cache_wait_timeout(clickhouse_query_settings, reader),
        timer=timer,
    )


def _get_cache_wait_timeout(
    query_settings: MutableMapping[str, Any], reader: Reader
) -> int:
    """
    Helper function to determine how long a query should wait when doing
    a readthrough caching.

    The overrides are primarily used for debugging the ExecutionTimeoutError
    raised by the readthrough caching system on the tigers cluster. When we
    have root caused the problem we can remove the overrides.
    """
    cache_wait_timeout: int = int(query_settings.get("max_execution_time", 30))
    if reader.cache_partition_id and reader.cache_partition_id in {
        "tiger_errors",
        "tiger_transactions",
    }:
        tiger_wait_timeout_config = state.get_config("tiger-cache-wait-time")
        if tiger_wait_timeout_config:
            cache_wait_timeout = tiger_wait_timeout_config
    return cache_wait_timeout


def _get_query_settings_from_config(
    override_prefix: Optional[str],
) -> MutableMapping[str, Any]:
    """
    Helper function to get the query settings from the config.

    #TODO: Make this configurable by entity/dataset. Since we want to use
    #      different settings across different clusters belonging to the
    #      same entity/dataset, using cache_partition right now. This is
    #      not ideal but it works for now.
    """
    all_confs = state.get_all_configs()

    # Populate the query settings with the default values
    clickhouse_query_settings: MutableMapping[str, Any] = {
        k.split("/", 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith("query_settings/")
    }

    if override_prefix:
        for k, v in all_confs.items():
            if k.startswith(f"{override_prefix}/query_settings/"):
                clickhouse_query_settings[k.split("/", 2)[2]] = v

    return clickhouse_query_settings


def raw_query(
    # TODO: Passing the whole clickhouse query here is needed as long
    # as the execute method depends on it. Otherwise we can make this
    # file rely either entirely on clickhouse query or entirely on
    # the formatter.
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    stats: MutableMapping[str, Any],
    trace_id: Optional[str] = None,
    robust: bool = False,
) -> QueryResult:
    """
    Submits a raw SQL query to the DB and does some post-processing on it to
    fix some of the formatting issues in the result JSON.
    This function is not supposed to depend on anything higher level than the clickhouse
    query. If this function ends up depending on the dataset, something is wrong.
    """
    clickhouse_query_settings = _get_query_settings_from_config(
        reader.get_query_settings_prefix()
    )

    timer.mark("get_configs")

    sql = formatted_query.get_sql()

    update_with_status = partial(
        update_query_metadata_and_stats,
        clickhouse_query,
        sql,
        timer,
        stats,
        query_metadata,
        clickhouse_query_settings,
        trace_id,
    )

    try:
        result = execute_query_with_query_id(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust=robust,
        )
    except Exception as cause:
        if isinstance(cause, RateLimitExceeded):
            trigger_rate_limiter = cause.extra_data.get("scope", "")
            stats = update_with_status(
                QueryStatus.RATE_LIMITED, triggered_rate_limiter=trigger_rate_limiter
            )
        else:
            error_code = None
            status = QueryStatus.ERROR
            with configure_scope() as scope:
                if isinstance(cause, ClickhouseError):
                    error_code = cause.code
                    fingerprint = [
                        "{{default}}",
                        str(cause.code),
                        query_metadata.dataset,
                    ]
                    if error_code not in constants.CLICKHOUSE_SYSTEMATIC_FAILURES:
                        fingerprint.append(query_metadata.request.referrer)

                    scope.fingerprint = fingerprint
                    if scope.span:
                        if cause.code == errors.ErrorCodes.TOO_SLOW:
                            sentry_sdk.set_tag("timeout", "predicted")
                            status = QueryStatus.TIMEOUT
                        elif cause.code == errors.ErrorCodes.TIMEOUT_EXCEEDED:
                            sentry_sdk.set_tag("timeout", "query_timeout")
                            status = QueryStatus.TIMEOUT
                        elif cause.code in (
                            errors.ErrorCodes.SOCKET_TIMEOUT,
                            errors.ErrorCodes.NETWORK_ERROR,
                        ):
                            sentry_sdk.set_tag("timeout", "network")
                elif isinstance(
                    cause,
                    (TimeoutError, ExecutionTimeoutError),
                ):
                    if scope.span:
                        sentry_sdk.set_tag("timeout", "cache_timeout")
                        status = QueryStatus.TIMEOUT

                logger.exception("Error running query: %s\n%s", sql, cause)
            stats = update_with_status(status, error_code=error_code)
        raise QueryException.from_args(
            # This exception needs to have the message of the cause in it for sentry
            # to pick it up properly
            str(cause),
            {
                "stats": stats,
                "sql": sql,
                "experiments": clickhouse_query.get_experiments(),
            },
        ) from cause
    else:
        stats = update_with_status(QueryStatus.SUCCESS, result["profile"])
        return QueryResult(
            result,
            {
                "stats": stats,
                "sql": sql,
                "experiments": clickhouse_query.get_experiments(),
            },
        )
