from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hashlib import md5
from threading import Lock
from typing import (
    Any,
    Dict,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Union,
    cast,
)

import rapidjson
import sentry_sdk
from clickhouse_driver.errors import ErrorCodes
from sentry_sdk import Hub
from sentry_sdk.api import configure_scope

from snuba import environment, state
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.formatter.query import format_query_anonymized
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range_estimate
from snuba.clickhouse.query_profiler import generate_profile
from snuba.query import ProcessableQuery
from snuba.query.allocation_policies import (
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicy,
    QueryResultOrError,
)
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    QueryStatus,
    RequestStatus,
    Status,
    get_query_status_from_error_codes,
    get_request_status,
)
from snuba.reader import Reader, Result
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.cache.abstract import Cache, ExecutionTimeoutError
from snuba.state.cache.redis.backend import RESULT_VALUE, RESULT_WAIT, RedisCache
from snuba.state.quota import ResourceQuota
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
    stats: Dict[str, Any],
    query_metadata_list: MutableSequence[ClickhouseQueryMetadata],
    query_settings: Mapping[str, Any],
    trace_id: Optional[str],
    status: QueryStatus,
    request_status: Status,
    profile_data: Optional[Dict[str, Any]] = None,
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

    query_metadata_list.append(
        ClickhouseQueryMetadata(
            sql=sql,
            sql_anonymized=sql_anonymized,
            start_timestamp=start,
            end_timestamp=end,
            stats=stats,
            status=status,
            request_status=request_status,
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
            e.code != ErrorCodes.QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING
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
    attribution_info: AttributionInfo,
    dataset_name: str,
    # NOTE: This variable is a piece of state which is updated and used outside this function
    query_metadata_list: MutableSequence[ClickhouseQueryMetadata],
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    # NOTE: This variable is a piece of state which is updated and used outside this function
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
        query=clickhouse_query,
        query_metadata_list=query_metadata_list,
        sql=sql,
        stats=stats,
        query_settings=clickhouse_query_settings,
        trace_id=trace_id,
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
        error_code = None
        trigger_rate_limiter = None
        status = None
        request_status = get_request_status(cause)
        if isinstance(cause, RateLimitExceeded):
            status = QueryStatus.RATE_LIMITED
            trigger_rate_limiter = cause.extra_data.get("scope", "")
        elif isinstance(cause, ClickhouseError):
            error_code = cause.code
            status = get_query_status_from_error_codes(error_code)

            with configure_scope() as scope:
                fingerprint = ["{{default}}", str(cause.code), dataset_name]
                if error_code not in constants.CLICKHOUSE_SYSTEMATIC_FAILURES:
                    fingerprint.append(attribution_info.referrer)
                scope.fingerprint = fingerprint
        elif isinstance(cause, TimeoutError):
            status = QueryStatus.TIMEOUT
        elif isinstance(cause, ExecutionTimeoutError):
            status = QueryStatus.TIMEOUT

        status = status or QueryStatus.ERROR
        if request_status.status not in (
            RequestStatus.TABLE_RATE_LIMITED,
            RequestStatus.RATE_LIMITED,
        ):
            # Don't log rate limiting errors to avoid clutter
            logger.exception("Error running query: %s\n%s", sql, cause)

        with configure_scope() as scope:
            if scope.span:
                sentry_sdk.set_tag("slo_status", request_status.status.value)

        stats = update_with_status(
            status=status,
            request_status=request_status,
            error_code=error_code,
            triggered_rate_limiter=str(trigger_rate_limiter),
        )
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
        stats = update_with_status(
            status=QueryStatus.SUCCESS,
            request_status=get_request_status(),
            profile_data=result["profile"],
        )
        return QueryResult(
            result,
            {
                "stats": stats,
                "sql": sql,
                "experiments": clickhouse_query.get_experiments(),
            },
        )


def _get_allocation_policy(
    clickhouse_query: Union[Query, CompositeQuery[Table]]
) -> AllocationPolicy:
    """FIXME: docstring, testing"""
    from_clause = clickhouse_query.get_from_clause()
    if isinstance(from_clause, Table):
        return from_clause.allocation_policy
    elif isinstance(from_clause, ProcessableQuery):
        return _get_allocation_policy(cast(Query, from_clause))
    elif isinstance(from_clause, CompositeQuery):
        return _get_allocation_policy(from_clause)
    elif isinstance(from_clause, JoinClause):
        # HACK (Volo): Joins are a weird case for allocation policies and we don't
        # actually use them anywhere so I'm purposefully just kicking this can down the
        # road
        return DEFAULT_PASSTHROUGH_POLICY
    else:
        # FIXME: make a custom exception
        raise Exception(f"Could not determine allocation policy for {clickhouse_query}")


def db_query(
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
    result = None
    error = None
    allocation_policy = _get_allocation_policy(clickhouse_query)
    quota_allowance = allocation_policy.get_quota_allowance(
        # FIXME (Volo): This is a bad way to pass in the attribution info,
        # we should just pass it in explicitly
        query_metadata.request.attribution_info.tenant_ids
    )
    if not quota_allowance.can_run:
        # TODO: Maybe this is better as an exception?
        # I don't like exception handling as control flow too much but it's
        # the pattern in this file already
        raise Exception("Tenant is over quota")
    query_settings.set_resource_quota(
        ResourceQuota(max_threads=quota_allowance.max_threads)
    )
    try:
        result = raw_query(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            query_metadata,
            stats,
            trace_id,
            robust,
        )
    except QueryException as e:
        error = e
    finally:
        allocation_policy.update_quota_balance(
            tenant_ids=query_metadata.request.attribution_info.tenant_ids,
            result_or_error=QueryResultOrError(query_result=result, error=error),
        )
        if result:
            return result
        raise error or Exception(
            "No error or result when running query, this should never happen"
        )
