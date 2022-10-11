from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial, reduce
from hashlib import md5
from threading import Lock
from typing import Any, Mapping, MutableMapping, Optional, Set, Union, cast

import rapidjson
import sentry_sdk
from clickhouse_driver import errors
from sentry_sdk import Hub
from sentry_sdk.api import configure_scope

from snuba import environment, settings, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.formatter.query import format_query_anonymized
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range_estimate
from snuba.clickhouse.query_profiler import generate_profile
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    QueryStatus,
    SnubaQueryMetadata,
)
from snuba.reader import Reader, Result
from snuba.redis import redis_client
from snuba.state.cache.abstract import (
    Cache,
    ExecutionTimeoutError,
    TigerExecutionTimeoutError,
)
from snuba.state.cache.redis.backend import RESULT_VALUE, RESULT_WAIT, RedisCache
from snuba.state.rate_limit import (
    GLOBAL_RATE_LIMIT_NAME,
    ORGANIZATION_RATE_LIMIT_NAME,
    PROJECT_RATE_LIMIT_NAME,
    TABLE_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitStatsContainer,
    get_global_rate_limit_params,
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

MAX_HASH_PLUS_ONE = 16**32  # Max value of md5 hash
metrics = MetricsWrapper(environment.metrics, "db_query")


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
        redis_client,
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


class ReferencedColumnsCounter(
    DataSourceVisitor[None, Table], JoinVisitor[None, Table]
):
    """
    Traverse a potentially composite data source tree for a query
    and count the physical Clickhouse columns referenced.

    Since get_all_ast_referenced_columns only returns the columns
    referenced directly by a query (ignoring subqueries), we need
    to traverse each node of the data source structure individually.
    """

    def __init__(self) -> None:
        # When we have a join node that references a table directly
        # instead of a subquery we pick the columns list from the
        # query that contains that join. So when visiting the join
        # node we signal that the enclosing query has to count the
        # columns for that join node.
        self.__incomplete_tables: MutableMapping[str, str] = {}
        # Analysed tables with their columns list
        self.__complete_tables: MutableMapping[str, Set[str]] = {}

    def count_columns(self) -> int:
        return reduce(
            lambda total, table: total + len(self.__complete_tables[table]),
            self.__complete_tables,
            0,
        )

    def _visit_simple_source(self, data_source: Table) -> None:
        return

    def _visit_join(self, data_source: JoinClause[Table]) -> None:
        self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> None:
        table = data_source.get_from_clause().table_name
        columns = set(
            (
                # Skip aliases when counting columns
                c.column_name
                for c in data_source.get_all_ast_referenced_columns()
            )
        )

        if table in self.__complete_tables:
            self.__complete_tables[table] |= columns
        else:
            self.__complete_tables[table] = columns

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())
        if self.__incomplete_tables:
            # This case means that we ran into a join node that referenced
            # a table directly. So that table, with its alias is in the
            # incomplete tables mapping and we count all the columns this
            # query references from there.
            for _, table in self.__incomplete_tables.items():
                if table not in self.__complete_tables:
                    self.__complete_tables[table] = set()
            for c in data_source.get_all_ast_referenced_columns():
                if (
                    c.table_name is not None
                    and c.table_name in self.__incomplete_tables
                ):
                    self.__complete_tables[self.__incomplete_tables[c.table_name]].add(
                        c.column_name
                    )
            self.__incomplete_tables = {}

    def visit_individual_node(self, node: IndividualNode[Table]) -> None:
        if isinstance(node.data_source, Table):
            # This is an individual table in a join. So we signal that the
            # enclosing query needs to count the columns that reference this
            # table.
            self.__incomplete_tables[node.alias] = node.data_source.table_name
        else:
            self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)


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
) -> MutableMapping[str, Any]:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """
    stats.update(query_settings)
    if error_code is not None:
        stats["error_code"] = error_code
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
    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in ClickHouse.
    uc_max = state.get_config("uncompressed_cache_max_cols", 5)
    assert isinstance(uc_max, int)
    column_counter = ReferencedColumnsCounter()
    column_counter.visit(clickhouse_query.get_from_clause())
    if column_counter.count_columns() > uc_max:
        clickhouse_query_settings["use_uncompressed_cache"] = 0

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
    global_rate_limit_stats = rate_limit_stats_container.get_stats(
        GLOBAL_RATE_LIMIT_NAME
    )
    if global_rate_limit_stats is not None:
        metrics.gauge(
            name="global_concurrent",
            value=global_rate_limit_stats.concurrent,
            tags={"table": stats.get("clickhouse_table", "")},
        )
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
    # Global rate limiter is added at the end of the chain to be
    # the last for evaluation.
    # This allows us not to borrow capacity from the global quota
    # during the evaluation if one of the more specific limiters
    # (like the project rate limiter) rejects the query first.
    if state.get_config("enable_global_rate_limiter", 1):
        query_settings.add_rate_limit(get_global_rate_limit_params())

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

        thread_quota = query_settings.get_resource_quota()
        if (
            ("max_threads" in clickhouse_query_settings or thread_quota is not None)
            and project_rate_limit_stats is not None
            and project_rate_limit_stats.concurrent > 1
        ):
            maxt = (
                clickhouse_query_settings["max_threads"]
                if thread_quota is None
                else thread_quota.max_threads
            )
            clickhouse_query_settings["max_threads"] = max(
                1, maxt - project_rate_limit_stats.concurrent + 1
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


def check_sorted_sql_key_in_cache(
    formatted_query_sorted: Optional[FormattedQuery],
    reader: Reader,
    sorted_key_cache_experiment: dict[str, bool],
) -> None:
    cache_partition = _get_cache_partition(reader)
    if formatted_query_sorted:
        key_sorted = get_query_cache_key(formatted_query_sorted)
        percentage = state.get_config(
            "sort_sql_fields_and_conditions_rollout_percentage", 0
        )
        assert isinstance(percentage, int)
        if hash_to_probability(key_sorted) < percentage:
            key_sorted_with_prefix = f"sorted:{key_sorted}"
            result = cache_partition.get(key_sorted_with_prefix)
            if result is not None:
                sorted_key_cache_experiment["is_selected"] = True
                sorted_key_cache_experiment["sorted_key_exists"] = True
            else:
                empty_result: Result = {
                    "meta": [],
                    "data": [],
                    "totals": {},
                    "profile": None,
                    "trace_output": "",
                }
                cache_partition.set(key_sorted_with_prefix, empty_result)
                sorted_key_cache_experiment["is_selected"] = True


def hash_to_probability(hexadecimal: str) -> float:
    return float(int(hexadecimal, 16) / MAX_HASH_PLUS_ONE) * 100


def write_sorted_unsorted_cache_hit_metric(
    sorted_key_cache_experiment: dict[str, bool], unsorted_key_exists: bool
) -> None:
    if sorted_key_cache_experiment["is_selected"]:
        sorted_key_exists = sorted_key_cache_experiment["sorted_key_exists"]
        metrics.increment(
            "sorted_and_unsorted_cache_hit",
            tags={"sorted/unsorted": f"{sorted_key_exists}/{unsorted_key_exists}"},
        )


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
                exception = (
                    TigerExecutionTimeoutError
                    if "tiger" in partition_id
                    else ExecutionTimeoutError
                )
                cache_partitions[partition_id] = RedisCache(
                    redis_client,
                    f"snuba-query-cache:{partition_id}:",
                    ResultCacheCodec(),
                    ThreadPoolExecutor(),
                    exception,
                )

    return cache_partitions[
        partition_id if partition_id is not None else DEFAULT_CACHE_PARTITION_ID
    ]


@with_span(op="db")
def execute_query_with_caching(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    formatted_query_sorted: Optional[FormattedQuery],
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
) -> Result:
    # XXX: ``uncompressed_cache_max_cols`` is used to control both the result
    # cache, as well as the uncompressed cache. These should be independent.
    use_cache, uc_max = state.get_configs(
        [("use_cache", settings.USE_RESULT_CACHE), ("uncompressed_cache_max_cols", 5)]
    )

    column_counter = ReferencedColumnsCounter()
    column_counter.visit(clickhouse_query.get_from_clause())
    assert isinstance(uc_max, int)
    if column_counter.count_columns() > uc_max:
        use_cache = False

    execute = partial(
        execute_query_with_rate_limits,
        clickhouse_query,
        query_settings,
        formatted_query,
        reader,
        timer,
        stats,
        clickhouse_query_settings,
        robust=robust,
    )

    with sentry_sdk.start_span(description="execute", op="db") as span:
        key = get_query_cache_key(formatted_query)
        clickhouse_query_settings["query_id"] = key
        sorted_key_cache_experiment = {"is_selected": False, "sorted_key_exists": False}
        if use_cache:
            try:
                check_sorted_sql_key_in_cache(
                    formatted_query_sorted, reader, sorted_key_cache_experiment
                )
            except Exception:
                pass
            cache_partition = _get_cache_partition(reader)
            result = cache_partition.get(key)
            timer.mark("cache_get")
            stats["cache_hit"] = result is not None
            if result is not None:
                span.set_tag("cache", "hit")
                write_sorted_unsorted_cache_hit_metric(
                    sorted_key_cache_experiment, True
                )
                return result

            span.set_tag("cache", "miss")
            write_sorted_unsorted_cache_hit_metric(sorted_key_cache_experiment, False)
            result = execute()
            cache_partition.set(key, result)
            timer.mark("cache_set")
            return result
        else:
            return execute()


@with_span(op="db")
def execute_query_with_readthrough_caching(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    formatted_query: FormattedQuery,
    formatted_query_sorted: Optional[FormattedQuery],
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    clickhouse_query_settings: MutableMapping[str, Any],
    robust: bool,
) -> Result:
    query_id = get_query_cache_key(formatted_query)
    sorted_key_cache_experiment = {"is_selected": False, "sorted_key_exists": False}
    try:
        check_sorted_sql_key_in_cache(
            formatted_query_sorted, reader, sorted_key_cache_experiment
        )
    except Exception:
        pass
    clickhouse_query_settings["query_id"] = query_id

    span = Hub.current.scope.span
    if span:
        span.set_data("query_id", query_id)

    def record_cache_hit_type(hit_type: int) -> None:
        span_tag = "cache_miss"
        if hit_type == RESULT_VALUE:
            stats["cache_hit"] = 1
            span_tag = "cache_hit"
            write_sorted_unsorted_cache_hit_metric(sorted_key_cache_experiment, True)
        elif hit_type == RESULT_WAIT:
            stats["is_duplicate"] = 1
            span_tag = "cache_wait"
        else:
            write_sorted_unsorted_cache_hit_metric(sorted_key_cache_experiment, False)
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
    formatted_query_sorted: Optional[FormattedQuery],
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

    execute_query_strategy = (
        execute_query_with_readthrough_caching
        if state.get_config("use_readthrough_query_cache", 1)
        else execute_query_with_caching
    )

    try:
        result = execute_query_strategy(
            clickhouse_query,
            query_settings,
            formatted_query,
            formatted_query_sorted,
            reader,
            timer,
            stats,
            clickhouse_query_settings,
            robust=robust,
        )
    except Exception as cause:
        if isinstance(cause, RateLimitExceeded):
            stats = update_with_status(QueryStatus.RATE_LIMITED)
        else:
            error_code = None
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
                        elif cause.code == errors.ErrorCodes.TIMEOUT_EXCEEDED:
                            sentry_sdk.set_tag("timeout", "query_timeout")
                        elif cause.code in (
                            errors.ErrorCodes.SOCKET_TIMEOUT,
                            errors.ErrorCodes.NETWORK_ERROR,
                        ):
                            sentry_sdk.set_tag("timeout", "network")
                elif isinstance(
                    cause,
                    (TimeoutError, ExecutionTimeoutError, TigerExecutionTimeoutError),
                ):
                    if scope.span:
                        sentry_sdk.set_tag("timeout", "cache_timeout")

                logger.exception("Error running query: %s\n%s", sql, cause)
            stats = update_with_status(QueryStatus.ERROR, error_code=error_code)
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
