from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial, reduce
from hashlib import md5
from typing import Any, Mapping, MutableMapping, Optional, Set

import sentry_sdk
from sentry_sdk.api import configure_scope
from snuba import settings, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_profiler import generate_profile
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.querylog.query_metadata import (
    ClickhouseQueryMetadata,
    QueryStatus,
    SnubaQueryMetadata,
)
from snuba.reader import Reader, Result
from snuba.redis import redis_client
from snuba.request.request_settings import RequestSettings
from snuba.state.cache.abstract import Cache
from snuba.state.cache.redis.backend import RedisCache
from snuba.state.rate_limit import (
    PROJECT_RATE_LIMIT_NAME,
    RateLimitAggregator,
    RateLimitExceeded,
)
from snuba.util import force_bytes, with_span
from snuba.utils.codecs import JSONCodec, JSONData
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException, QueryResult

cache: Cache[JSONData] = RedisCache(
    redis_client, "snuba-query-cache:", JSONCodec(), ThreadPoolExecutor()
)

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
) -> MutableMapping[str, Any]:
    """
    If query logging is enabled then logs details about the query and its status, as
    well as timing information.
    Also updates stats with any relevant information and returns the updated dict.
    """
    stats.update(query_settings)

    query_metadata.query_list.append(
        ClickhouseQueryMetadata(
            sql=sql,
            stats=stats,
            status=status,
            profile=generate_profile(query),
            trace_id=trace_id,
        )
    )

    return stats


@with_span(op="db")
def execute_query(
    # TODO: Passing the whole clickhouse query here is needed as long
    # as the execute method depends on it. Otherwise we can make this
    # file rely either entirely on clickhouse query or entirely on
    # the formatter.
    clickhouse_query: Query,
    request_settings: RequestSettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_settings: MutableMapping[str, Any],
) -> Result:
    """
    Execute a query and return a result.
    """
    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in ClickHouse.
    uc_max = state.get_config("uncompressed_cache_max_cols", 5)
    column_counter = ReferencedColumnsCounter()
    column_counter.visit(clickhouse_query.get_from_clause())
    if column_counter.count_columns() > uc_max:
        query_settings["use_uncompressed_cache"] = 0

    # Force query to use the first shard replica, which
    # should have synchronously received any cluster writes
    # before this query is run.
    consistent = request_settings.get_consistent()
    stats["consistent"] = consistent
    if consistent:
        query_settings["load_balancing"] = "in_order"
        query_settings["max_threads"] = 1

    result = reader.execute(
        formatted_query, query_settings, with_totals=clickhouse_query.has_totals(),
    )

    timer.mark("execute")
    stats.update(
        {"result_rows": len(result["data"]), "result_cols": len(result["meta"])}
    )

    return result


@with_span(op="db")
def execute_query_with_rate_limits(
    clickhouse_query: Query,
    request_settings: RequestSettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_settings: MutableMapping[str, Any],
) -> Result:
    # XXX: We should consider moving this that it applies to the logical query,
    # not the physical query.
    with RateLimitAggregator(
        request_settings.get_rate_limit_params()
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

        return execute_query(
            clickhouse_query,
            request_settings,
            formatted_query,
            reader,
            timer,
            stats,
            query_settings,
        )


def get_query_cache_key(formatted_query: FormattedQuery) -> str:
    return md5(force_bytes(formatted_query.get_sql())).hexdigest()


@with_span(op="db")
def execute_query_with_caching(
    clickhouse_query: Query,
    request_settings: RequestSettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_settings: MutableMapping[str, Any],
) -> Result:
    # XXX: ``uncompressed_cache_max_cols`` is used to control both the result
    # cache, as well as the uncompressed cache. These should be independent.
    use_cache, uc_max = state.get_configs(
        [("use_cache", settings.USE_RESULT_CACHE), ("uncompressed_cache_max_cols", 5)]
    )

    column_counter = ReferencedColumnsCounter()
    column_counter.visit(clickhouse_query.get_from_clause())
    if column_counter.count_columns() > uc_max:
        use_cache = False

    execute = partial(
        execute_query_with_rate_limits,
        clickhouse_query,
        request_settings,
        formatted_query,
        reader,
        timer,
        stats,
        query_settings,
    )

    with sentry_sdk.start_span(description="execute", op="db") as span:
        if use_cache:
            key = get_query_cache_key(formatted_query)
            result = cache.get(key)
            timer.mark("cache_get")
            stats["cache_hit"] = result is not None
            if result is not None:
                span.set_tag("cache", "hit")
                return result

            span.set_tag("cache", "miss")
            result = execute()
            cache.set(key, result)
            timer.mark("cache_set")
            return result
        else:
            return execute()


@with_span(op="db")
def execute_query_with_readthrough_caching(
    clickhouse_query: Query,
    request_settings: RequestSettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    stats: MutableMapping[str, Any],
    query_settings: MutableMapping[str, Any],
) -> Result:
    query_id = get_query_cache_key(formatted_query)
    query_settings["query_id"] = query_id
    return cache.get_readthrough(
        query_id,
        partial(
            execute_query_with_rate_limits,
            clickhouse_query,
            request_settings,
            formatted_query,
            reader,
            timer,
            stats,
            query_settings,
        ),
        timeout=query_settings.get("max_execution_time", 30),
        timer=timer,
    )


def raw_query(
    # TODO: Passing the whole clickhouse query here is needed as long
    # as the execute method depends on it. Otherwise we can make this
    # file rely either entirely on clickhouse query or entirely on
    # the formatter.
    clickhouse_query: Query,
    request_settings: RequestSettings,
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    stats: MutableMapping[str, Any],
    trace_id: Optional[str] = None,
) -> QueryResult:
    """
    Submits a raw SQL query to the DB and does some post-processing on it to
    fix some of the formatting issues in the result JSON.
    This function is not supposed to depend on anything higher level than the clickhouse
    query. If this function ends up depending on the dataset, something is wrong.
    """
    all_confs = state.get_all_configs()
    query_settings: MutableMapping[str, Any] = {
        k.split("/", 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith("query_settings/")
    }

    timer.mark("get_configs")

    sql = formatted_query.get_sql()

    update_with_status = partial(
        update_query_metadata_and_stats,
        clickhouse_query,
        sql,
        timer,
        stats,
        query_metadata,
        query_settings,
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
            request_settings,
            formatted_query,
            reader,
            timer,
            stats,
            query_settings,
        )
    except Exception as cause:
        if isinstance(cause, RateLimitExceeded):
            stats = update_with_status(QueryStatus.RATE_LIMITED)
        else:
            with configure_scope() as scope:
                if isinstance(cause, ClickhouseError):
                    scope.fingerprint = ["{{default}}", str(cause.code)]
                logger.exception("Error running query: %s\n%s", sql, cause)
            stats = update_with_status(QueryStatus.ERROR)
        raise QueryException({"stats": stats, "sql": sql}) from cause
    else:
        stats = update_with_status(QueryStatus.SUCCESS)
        return QueryResult(result, {"stats": stats, "sql": sql})
