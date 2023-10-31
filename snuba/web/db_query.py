from __future__ import annotations

import logging
import random
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

from snuba import environment, settings, state
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
    AllocationPolicyViolation,
    AllocationPolicyViolations,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import (
    SLO,
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
    TABLE_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitStatsContainer,
)
from snuba.util import force_bytes
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
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
    query: Union[Query, CompositeQuery[Table]],
    sql: str,
    stats: MutableMapping[str, Any],
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
    start, end = get_time_range_estimate(cast(ProcessableQuery[Table], query))

    query_metadata_list.append(
        ClickhouseQueryMetadata(
            sql=sql,
            sql_anonymized=sql_anonymized,
            start_timestamp=start,
            end_timestamp=end,
            stats=dict(stats),
            status=status,
            request_status=request_status,
            profile=generate_profile(query),
            trace_id=trace_id,
            result_profile=profile_data,
        )
    )
    return stats


@with_span(op="function")
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
    # Apply clickhouse query setting overrides
    clickhouse_query_settings.update(query_settings.get_clickhouse_settings())

    result = reader.execute(
        formatted_query,
        clickhouse_query_settings,
        with_totals=clickhouse_query.has_totals(),
        robust=robust,
    )

    timer.mark("execute")
    stats.update(
        {
            "result_rows": len(result["data"]),
            "result_cols": len(result["meta"]),
            "max_threads": clickhouse_query_settings.get("max_threads", None),
        }
    )

    return result


def _record_rate_limit_metrics(
    rate_limit_stats_container: RateLimitStatsContainer,
    reader: Reader,
    stats: MutableMapping[str, Any],
) -> None:
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


@with_span(op="function")
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


@with_span(op="function")
def execute_query_with_query_id(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
    referrer: str,
) -> Result:

    if state.get_config("randomize_query_id", False):
        query_id = uuid.uuid4().hex
    else:
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
            referrer,
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
            referrer,
        )


@with_span(op="function")
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
    referrer: str,
) -> Result:

    span = Hub.current.scope.span

    if referrer in settings.BYPASS_CACHE_REFERRERS and state.get_config(
        "enable_bypass_cache_referrers"
    ):
        query_id = f"randomized-{uuid.uuid4().hex}"
        clickhouse_query_settings["query_id"] = query_id
        if span:
            span.set_data("query_id", query_id)

        return execute_query_with_rate_limits(
            clickhouse_query,
            query_settings,
            formatted_query,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust,
        )

    clickhouse_query_settings["query_id"] = query_id

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
    async_override: bool,
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

    if async_override:
        for k, v in all_confs.items():
            if k.startswith("async_query_settings/"):
                clickhouse_query_settings[k.split("/", 1)[1]] = v

    if override_prefix:
        for k, v in all_confs.items():
            if k.startswith(f"{override_prefix}/query_settings/"):
                clickhouse_query_settings[k.split("/", 2)[2]] = v

    return clickhouse_query_settings


def _raw_query(
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
    this function is responsible for running the clickhouse query and if there is any error, constructing the
    QueryException that  the rest of the stack depends on. See the `db_query` docstring for more details
    """
    clickhouse_query_settings = _get_query_settings_from_config(
        reader.get_query_settings_prefix(), query_settings.get_asynchronous()
    )
    resource_quota = query_settings.get_resource_quota()
    max_threads = resource_quota.max_threads if resource_quota else None
    if max_threads:
        clickhouse_query_settings["max_threads"] = max_threads
    timer.mark("get_configs")

    sql = formatted_query.get_sql()

    # Force query to use the first shard replica, which
    # should have synchronously received any cluster writes
    # before this query is run.
    consistent = query_settings.get_consistent()
    stats["consistent"] = consistent
    if consistent:
        sample_rate = state.get_config(
            f"{dataset_name}_ignore_consistent_queries_sample_rate", 0
        )
        assert sample_rate is not None
        ignore_consistent = random.random() < float(sample_rate)
        if not ignore_consistent:
            clickhouse_query_settings["load_balancing"] = "in_order"
            clickhouse_query_settings["max_threads"] = 1
        else:
            stats["consistent"] = False
            metrics.increment(
                "ignored_consistent_queries",
                tags={"dataset": dataset_name, "referrer": attribution_info.referrer},
            )

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
            referrer=attribution_info.referrer,
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

        if request_status.slo == SLO.AGAINST:
            logger.exception("Error running query: %s\n%s", sql, cause)

        with configure_scope() as scope:
            if scope.span:
                sentry_sdk.set_tag("slo_status", request_status.status.value)

        stats = update_with_status(
            status=status or QueryStatus.ERROR,
            request_status=request_status,
            error_code=error_code,
            triggered_rate_limiter=str(trigger_rate_limiter),
        )
        raise QueryException.from_args(
            # This exception needs to have the message of the cause in it for sentry
            # to pick it up properly
            cause.__class__.__name__,
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


def _get_allocation_policies(
    clickhouse_query: Union[Query, CompositeQuery[Table]]
) -> list[AllocationPolicy]:
    """given a query, find the allocation policies in its from clause, in the case
    of CompositeQuery, follow the from clause until something is querying from a table
    and use that table's allocation policies.

    **GOTCHAS**
        - Does not handle joins, will return [PassthroughPolicy]
        - In case of error, returns [PassthroughPolicy], fails quietly (but logs to sentry)
    """
    from_clause = clickhouse_query.get_from_clause()
    if isinstance(from_clause, Table):
        return from_clause.allocation_policies
    elif isinstance(from_clause, ProcessableQuery):
        return _get_allocation_policies(cast(Query, from_clause))
    elif isinstance(from_clause, CompositeQuery):
        return _get_allocation_policies(from_clause)
    elif isinstance(from_clause, JoinClause):
        # HACK (Volo): Joins are a weird case for allocation policies and we don't
        # actually use them anywhere so I'm purposefully just kicking this can down the
        # road
        return [DEFAULT_PASSTHROUGH_POLICY]
    else:
        logger.exception(
            f"Could not determine allocation policies for {clickhouse_query}"
        )
        return [DEFAULT_PASSTHROUGH_POLICY]


def db_query(
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
    """This function is responsible for:

    * Checking and updating the allocation policy (which exists on the query)
    * Running the query on clickhouse with readthrough caching
    * applying rate limits which have been applied during the query pipeline
    * collecting information about the query which will become part of the querylog entry for this request
        * this is done with the stats, and query_metadata_list parameters


    ** GOTCHAS **
    --------------

    * Whenever something goes wrong during the running of this function, it is wrapped in a QueryException,
        that exception neeeds to have whatever stats were collected during this function's execution
        because the caller writes that information to the querylog. The cause of the QueryException
        is also read at the very top level of this application (snuba/web/views.py) to decide
        what status code to send back to the service caller. Changing that mechanism would mean
        changing those layers as well
    * The readthrough cache accepts an arbitary function to run with a readthrough redis cache. Currently
        it is applied around the rate limiting function but not the allocation policy.
        The layers look like this:

            --> db_query
                --> allocation policy
                    --> ...irrelevant stuff
                        --> execute_query_with_readthrough_caching
                            ### READTHROUGH CACHE GOES HERE ###
                                --> execute_query_with_rate_limits
                                    --> execute_query

        The implication is that if a user hits the cache they will not be rate limited because the
        request will simply be cached. That is the behavior at time of writing (28-03-2023) but there
        is no specific reason it has to be that way. If the ordering needs to be changed as the application
        evolves it can be changed. The inconsistency was consciously chosen for expediency and to have
        allocation policy be applied at the top level of the db_query process
    """

    allocation_policies = _get_allocation_policies(clickhouse_query)
    query_id = uuid.uuid4().hex
    result = None
    error = None

    try:
        _apply_allocation_policies_quota(
            query_settings,
            attribution_info,
            formatted_query,
            stats,
            allocation_policies,
            query_id,
        )
        result = _raw_query(
            clickhouse_query,
            query_settings,
            attribution_info,
            dataset_name,
            query_metadata_list,
            formatted_query,
            reader,
            timer,
            stats,
            trace_id,
            robust,
        )
    except AllocationPolicyViolations as e:
        update_query_metadata_and_stats(
            query=clickhouse_query,
            sql=formatted_query.get_sql(),
            stats=stats,
            query_metadata_list=query_metadata_list,
            query_settings={},
            trace_id=trace_id,
            status=QueryStatus.RATE_LIMITED,
            request_status=Status(RequestStatus.RATE_LIMITED),
            profile_data=None,
            error_code=None,
            triggered_rate_limiter="AllocationPolicy",
        )
        error = QueryException()
        error.__cause__ = e
        for allocation_policy in allocation_policies:
            allocation_policy.update_quota_balance(
                tenant_ids=attribution_info.tenant_ids,
                query_id=query_id,
                result_or_error=QueryResultOrError(query_result=None, error=error),
            )

        error = QueryException.from_args(
            AllocationPolicyViolations.__name__,
            "Query cannot be run due to allocation policies",
            extra={
                "stats": stats,
                "sql": "no sql run",
                "experiments": {},
            },
        )
        error.__cause__ = e
    except QueryException as e:
        error = e
    except Exception as e:
        # We count on _raw_query capturing all exceptions in a QueryException
        # if it didn't do that, something is very wrong so we just panic out here
        raise e
    finally:
        for allocation_policy in allocation_policies:
            allocation_policy.update_quota_balance(
                tenant_ids=attribution_info.tenant_ids,
                query_id=query_id,
                result_or_error=QueryResultOrError(query_result=result, error=error),
            )
        if result:
            return result
        raise error or Exception(
            "No error or result when running query, this should never happen"
        )


def _apply_allocation_policies_quota(
    query_settings: QuerySettings,
    attribution_info: AttributionInfo,
    formatted_query: FormattedQuery,
    stats: MutableMapping[str, Any],
    allocation_policies: list[AllocationPolicy],
    query_id: str,
) -> None:
    """
    Sets the resource quota in the query_settings object to the minimum of all available
    quota allowances from the given allocation policies.
    """
    quota_allowances: dict[str, QuotaAllowance] = {}
    violations: dict[str, AllocationPolicyViolation] = {}
    for allocation_policy in allocation_policies:
        try:
            quota_allowances[
                allocation_policy.config_key()
            ] = allocation_policy.get_quota_allowance(
                attribution_info.tenant_ids, query_id
            )

        except AllocationPolicyViolation as e:
            violations[allocation_policy.config_key()] = e
    if violations:
        stats["quota_allowance"] = {k: v.quota_allowance for k, v in violations.items()}
        # HACK: This is because our SLOs are calculated weirdly
        raise AllocationPolicyViolations(
            "Query cannot be run due to allocation policies", violations
        )

    stats["quota_allowance"] = {k: v.to_dict() for k, v in quota_allowances.items()}

    # Before allocation policies were a thing, the query pipeline would apply
    # thread limits in a query processor. That is not necessary if there
    # is an allocation_policy in place but nobody has removed that code yet.
    # Therefore, the least permissive thread limit is taken
    query_settings.set_resource_quota(
        ResourceQuota(
            max_threads=min(
                min(quota_allowances.values()).max_threads,
                getattr(query_settings.get_resource_quota(), "max_threads", 10),
            )
        )
    )
