import logging

from functools import partial
from hashlib import md5
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Optional,
)

from snuba import settings, state
from snuba.clickhouse.query import ClickhouseQuery
from snuba.environment import reader
from snuba.reader import Result
from snuba.redis import redis_client
from snuba.request import Request
from snuba.state.cache import Cache, RedisCache
from snuba.state.rate_limit import (
    PROJECT_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
)
from snuba.util import force_bytes
from snuba.utils.codecs import JSONCodec
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException, QueryResult
from snuba.web.query_metadata import ClickhouseQueryMetadata, SnubaQueryMetadata

cache: Cache[Any] = RedisCache(redis_client, "snuba-query-cache:", JSONCodec())

logger = logging.getLogger("snuba.query")


def update_query_metadata_and_stats(
    request: Request,
    sql: str,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_metadata: SnubaQueryMetadata,
    query_settings: Mapping[str, Any],
    trace_id: Optional[str],
    status: str,
) -> MutableMapping[str, Any]:
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


def execute_query(
    request: Request,
    query: ClickhouseQuery,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_settings: MutableMapping[str, Any],
    query_id: Optional[str] = None,
) -> Result:
    """
    Execute a query and return a result.
    """
    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in ClickHouse.
    uc_max = state.get_config("uncompressed_cache_max_cols", 5)
    if len(request.query.get_all_referenced_columns()) > uc_max:
        query_settings["use_uncompressed_cache"] = 0

    # Force query to use the first shard replica, which
    # should have synchronously received any cluster writes
    # before this query is run.
    consistent = request.settings.get_consistent()
    stats["consistent"] = consistent
    if consistent:
        query_settings["load_balancing"] = "in_order"
        query_settings["max_threads"] = 1

    result = reader.execute(
        query,
        query_settings,
        query_id=query_id,
        with_totals=request.query.has_totals(),
    )

    timer.mark("execute")
    stats.update(
        {"result_rows": len(result["data"]), "result_cols": len(result["meta"])}
    )

    return result


def raw_query(
    request: Request,
    query: ClickhouseQuery,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    stats: MutableMapping[str, Any],
    trace_id: Optional[str] = None,
) -> QueryResult:
    """
    Submits a raw SQL query to the DB and does some post-processing on it to
    fix some of the formatting issues in the result JSON.
    This function is not supposed to depend on anything higher level than the storage
    query (ClickhouseQuery as of now). If this function ends up depending on the
    dataset, something is wrong.

    TODO: As soon as we have a StorageQuery abstraction remove all the references
    to the original query from the request.
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
    # don't use result cache.
    if len(request.query.get_all_referenced_columns()) > uc_max:
        use_cache = 0

    timer.mark("get_configs")

    sql = query.format_sql()
    query_id = md5(force_bytes(sql)).hexdigest()

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

    try:
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

                    result = execute_query(
                        request,
                        query,
                        timer,
                        stats,
                        query_settings,
                        # All queries should already be deduplicated at this point
                        # But the query_id will let us know if they aren't
                        query_id=(query_id if use_deduper else None),
                    )

                if use_cache:
                    cache.set(query_id, result)
                    timer.mark("cache_set")
    except Exception as cause:
        if isinstance(cause, RateLimitExceeded):
            stats = update_with_status("rate-limited")
        else:
            logger.exception("Error running query: %s\n%s", sql, cause)
            stats = update_with_status("error")
        raise QueryException({"stats": stats, "sql": sql}) from cause
    else:
        stats = update_with_status("success")
        return QueryResult(result, {"stats": stats, "sql": sql})
