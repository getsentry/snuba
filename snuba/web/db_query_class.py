from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hashlib import md5
from random import random
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

DEFAULT_CACHE_PARTITION_ID = "default"

# This lock prevents us from initializing the cache twice. The cache is initialized
# with a thread pool. In case of race condition we could create the threads twice which
# is a waste.
cache_partitions_lock = Lock()

logger = logging.getLogger("snuba.query")


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


class DBQuery:
    def __init__(
        self,
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
    ) -> None:
        self.clickhouse_query = clickhouse_query
        self.query_settings = query_settings
        self.attribution_info = attribution_info
        self.dataset_name = dataset_name
        self.query_metadata_list = query_metadata_list
        self.sql = formatted_query.get_sql()
        self.reader = reader
        self.timer = timer
        self.stats = stats
        self.trace_id = trace_id
        self.robust = robust
        self.query_id = md5(force_bytes(self.sql)).hexdigest()
        self.allocation_policies: list[AllocationPolicy] = [DEFAULT_PASSTHROUGH_POLICY]

        self.cache_partitions: MutableMapping[str, Cache[Result]] = {
            DEFAULT_CACHE_PARTITION_ID: RedisCache(
                redis_cache_client,
                "snuba-query-cache:",
                ResultCacheCodec(),
                ThreadPoolExecutor(),
            )
        }

    @with_span(op="db")
    def execute(self) -> QueryResult:

        self._get_query_settings_from_config()

        cached_result = self._get_cached_query_result()
        if cached_result is not None:
            return cached_result

        self._get_allocation_policies()
        self._apply_allocation_policy_quotas()

        apply_table_rate_limits()

        result = execute_query()

        set_cached_query_result()

        return result

    def _get_allocation_policies(
        self, clickhouse_query: Union[Query, CompositeQuery[Table]] | None = None
    ) -> None:
        """given a query, find the allocation policies in its from clause, in the case
        of CompositeQuery, follow the from clause until something is querying from a table
        and use that table's allocation policies.

        **GOTCHAS**
            - Does not handle joins, will return [PassthroughPolicy]
            - In case of error, returns [PassthroughPolicy], fails quietly (but logs to sentry)
        """
        clickhouse_query = clickhouse_query or self.clickhouse_query
        from_clause = clickhouse_query.get_from_clause()
        if isinstance(from_clause, Table):
            self.allocation_policies = from_clause.allocation_policies
        elif isinstance(from_clause, ProcessableQuery):
            self._get_allocation_policies(cast(Query, from_clause))
        elif isinstance(from_clause, CompositeQuery):
            self._get_allocation_policies(from_clause)
        elif isinstance(from_clause, JoinClause):
            # HACK (Volo): Joins are a weird case for allocation policies and we don't
            # actually use them anywhere so I'm purposefully just kicking this can down the
            # road
            pass
        else:
            logger.exception(
                f"Could not determine allocation policies for {clickhouse_query}"
            )

    @with_span(op="capacity_management")
    def _apply_allocation_policy_quotas(self) -> None:
        """
        Sets the resource quota in the query_settings object to the minimum of all available
        quota allowances from the given allocation policies.
        """
        quota_allowances: dict[str, QuotaAllowance] = {}
        violations: dict[str, AllocationPolicyViolation] = {}
        for allocation_policy in self.allocation_policies:
            try:
                quota_allowances[
                    allocation_policy.config_key()
                ] = allocation_policy.get_quota_allowance(
                    self.attribution_info.tenant_ids
                )

            except AllocationPolicyViolation as e:
                violations[allocation_policy.config_key()] = e
        if violations:
            self.stats["quota_allowance"] = {
                k: v.quota_allowance for k, v in violations.items()
            }
            raise QueryException.from_args(
                AllocationPolicyViolations.__name__,
                "Query cannot be run due to allocation policies",
                extra={
                    "stats": self.stats,
                    "sql": self.sql,
                    "experiments": {},
                },
            ) from AllocationPolicyViolations(
                "Query cannot be run due to allocation policies", violations
            )

        self.stats["quota_allowance"] = {
            k: v.to_dict() for k, v in quota_allowances.items()
        }

        # Before allocation policies were a thing, the query pipeline would apply
        # thread limits in a query processor. That is not necessary if there
        # is an allocation_policy in place but nobody has removed that code yet.
        # Therefore, the least permissive thread limit is taken
        self.query_settings.set_resource_quota(
            ResourceQuota(
                max_threads=min(
                    min(quota_allowances.values()).max_threads,
                    getattr(
                        self.query_settings.get_resource_quota(), "max_threads", 10
                    ),
                )
            )
        )

    def _get_query_settings_from_config(
        self,
    ) -> None:
        """
        Helper function to get the query settings from the config.

        #TODO: Make this configurable by entity/dataset. Since we want to use
        #      different settings across different clusters belonging to the
        #      same entity/dataset, using cache_partition right now. This is
        #      not ideal but it works for now.
        """
        all_confs = state.get_all_configs()

        # Populate the query settings with the default values
        self.clickhouse_query_settings: MutableMapping[str, Any] = {
            k.split("/", 1)[1]: v
            for k, v in all_confs.items()
            if k.startswith("query_settings/")
        }

        if override_prefix := self.reader.get_query_settings_prefix():
            for k, v in all_confs.items():
                if k.startswith(f"{override_prefix}/query_settings/"):
                    self.clickhouse_query_settings[k.split("/", 2)[2]] = v

    @with_span(op="db")
    def _get_cached_query_result(self) -> QueryResult | None:
        span = Hub.current.scope.span
        if span:
            span.set_data("query_id", self.query_id)
        try:
            result = self._get_cache_partition().get_cached_result_and_record_timer(
                self.query_id, self.timer
            )
            self.stats["cache_hit"] = 1
            span_tag = "cache_hit"
            sentry_sdk.set_tag("cache_status", span_tag)
            if span:
                span.set_data("cache_status", span_tag)
        except Exception as cause:
            request_status = get_request_status(cause)
            sql = self.sql
            if request_status.slo == SLO.AGAINST:
                logger.exception("Error running query: %s\n%s", sql, cause)

            with configure_scope() as scope:
                if scope.span:
                    sentry_sdk.set_tag("slo_status", request_status.status.value)

            self._update_stats_and_metadata(
                status=QueryStatus.ERROR,
                request_status=request_status,
            )
            raise QueryException.from_args(
                # This exception needs to have the message of the cause in it for sentry
                # to pick it up properly
                cause.__class__.__name__,
                str(cause),
                {
                    "stats": self.stats,
                    "sql": sql,
                    "experiments": self.clickhouse_query.get_experiments(),
                },
            ) from cause
        else:
            if result is None:
                return None
            self._update_stats_and_metadata(
                status=QueryStatus.SUCCESS,
                request_status=get_request_status(),
                profile_data=result["profile"],
            )
            return QueryResult(
                result,
                {
                    "stats": self.stats,
                    "sql": self.sql,
                    "experiments": self.clickhouse_query.get_experiments(),
                },
            )

    def _update_stats_and_metadata(
        self,
        status: QueryStatus,
        request_status: Status,
        profile_data: Optional[Dict[str, Any]] = None,
        error_code: Optional[int] = None,
        triggered_rate_limiter: Optional[str] = None,
    ) -> None:
        """
        If query logging is enabled then logs details about the query and its status, as
        well as timing information.
        Also updates stats with any relevant information and returns the updated dict.
        """
        self.stats.update(self.clickhouse_query_settings)
        if error_code is not None:
            self.stats["error_code"] = error_code
        if triggered_rate_limiter is not None:
            self.stats["triggered_rate_limiter"] = triggered_rate_limiter
        sql_anonymized = format_query_anonymized(self.clickhouse_query).get_sql()
        start, end = get_time_range_estimate(self.clickhouse_query)  # type: ignore

        self.query_metadata_list.append(
            ClickhouseQueryMetadata(
                sql=self.sql,
                sql_anonymized=sql_anonymized,
                start_timestamp=start,
                end_timestamp=end,
                stats=dict(self.stats),
                status=status,
                request_status=request_status,
                profile=generate_profile(self.clickhouse_query),
                trace_id=self.trace_id,
                result_profile=profile_data,
            )
        )

    def _get_cache_partition(self) -> Cache[Result]:
        enable_cache_partitioning = state.get_config("enable_cache_partitioning", 1)
        if not enable_cache_partitioning:
            return self.cache_partitions[DEFAULT_CACHE_PARTITION_ID]

        partition_id = self.reader.cache_partition_id
        if partition_id is not None and partition_id not in self.cache_partitions:
            with cache_partitions_lock:
                # This condition was checked before as this lock should be acquired only
                # during the first query. So, for the vast majority of queries, the overhead
                # of acquiring the lock is not needed.
                if partition_id not in self.cache_partitions:
                    self.cache_partitions[partition_id] = RedisCache(
                        redis_cache_client,
                        f"snuba-query-cache:{partition_id}:",
                        ResultCacheCodec(),
                        ThreadPoolExecutor(),
                    )

        partition = self.cache_partitions[
            partition_id if partition_id is not None else DEFAULT_CACHE_PARTITION_ID
        ]
        metrics.increment(
            "cache_partition_loaded",
            tags={"partition_id": self.reader.cache_partition_id or "default"},
        )
        return partition
