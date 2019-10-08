import logging

from clickhouse_driver.errors import Error as ClickHouseError
from collections import namedtuple
from hashlib import md5
from typing import Any, MutableMapping, NamedTuple

from snuba import settings, state
from snuba.clickhouse.native import ClickhousePool
from snuba.clickhouse.query import ClickhouseQuery
from snuba.request import Request
from snuba.util import (
    all_referenced_columns,
    create_metrics,
    force_bytes,
    Timer,
    to_list,
)

logger = logging.getLogger('snuba.query')
metrics = create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, 'snuba.api')


class QueryResult(NamedTuple):
    # TODO: Give a better abstraction to QueryResult
    result: MutableMapping[str, MutableMapping[str, Any]]
    status: int


def raw_query(
    request: Request,
    query: ClickhouseQuery,
    client: ClickhousePool,
    timer: Timer,
    stats=None,
) -> QueryResult:
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """
    from snuba.clickhouse.native import NativeDriverReader

    project_ids = to_list(request.extensions['project']['project'])
    project_id = project_ids[0] if project_ids else 0  # TODO rate limit on every project in the list?
    stats = stats or {}
    grl, gcl, prl, pcl, use_cache, use_deduper, uc_max = state.get_configs([
        ('global_per_second_limit', None),
        ('global_concurrent_limit', 1000),
        ('project_per_second_limit', 1000),
        ('project_concurrent_limit', 1000),
        ('use_cache', 0),
        ('use_deduper', 1),
        ('uncompressed_cache_max_cols', 5),
    ])

    # Specific projects can have their rate limits overridden
    prl, pcl = state.get_configs([
        ('project_per_second_limit_{}'.format(project_id), prl),
        ('project_concurrent_limit_{}'.format(project_id), pcl),
    ])

    all_confs = state.get_all_configs()
    query_settings = {
        k.split('/', 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith('query_settings/')
    }

    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in clickhouse, or result cache in snuba.
    if len(all_referenced_columns(request.query.get_body())) > uc_max:
        query_settings['use_uncompressed_cache'] = 0
        use_cache = 0

    timer.mark('get_configs')

    sql = query.format_sql()
    query_id = md5(force_bytes(sql)).hexdigest()
    with state.deduper(query_id if use_deduper else None) as is_dupe:
        timer.mark('dedupe_wait')

        result = state.get_result(query_id) if use_cache else None
        timer.mark('cache_get')

        stats.update({
            'is_duplicate': is_dupe,
            'query_id': query_id,
            'use_cache': bool(use_cache),
            'cache_hit': bool(result)}
        ),

        if result:
            status = 200
        else:
            with state.rate_limit('global', grl, gcl) as (g_allowed, g_rate, g_concurr):
                metrics.gauge('query.global_concurrent', g_concurr)
                stats.update({'global_rate': g_rate, 'global_concurrent': g_concurr})

                with state.rate_limit(project_id, prl, pcl) as (p_allowed, p_rate, p_concurr):
                    stats.update({'project_rate': p_rate, 'project_concurrent': p_concurr})
                    timer.mark('rate_limit')

                    if g_allowed and p_allowed:

                        # Experiment, reduce max threads by 1 for each extra concurrent query
                        # that a project has running beyond the first one
                        if 'max_threads' in query_settings and p_concurr > 1:
                            maxt = query_settings['max_threads']
                            query_settings['max_threads'] = max(1, maxt - p_concurr + 1)

                        # Force query to use the first shard replica, which
                        # should have synchronously received any cluster writes
                        # before this query is run.
                        consistent = request.settings.consistent
                        stats['consistent'] = consistent
                        if consistent:
                            query_settings['load_balancing'] = 'in_order'
                            query_settings['max_threads'] = 1

                        try:
                            result = NativeDriverReader(client).execute(
                                query,
                                query_settings,
                                # All queries should already be deduplicated at this point
                                # But the query_id will let us know if they aren't
                                query_id=query_id if use_deduper else None,
                                with_totals=request.query.has_totals(),
                            )
                            status = 200

                            logger.debug(sql)
                            timer.mark('execute')
                            stats.update({
                                'result_rows': len(result['data']),
                                'result_cols': len(result['meta']),
                            })

                            if use_cache:
                                state.set_result(query_id, result)
                                timer.mark('cache_set')

                        except BaseException as ex:
                            error = str(ex)
                            status = 500
                            logger.exception("Error running query: %s\n%s", sql, error)
                            if isinstance(ex, ClickHouseError):
                                result = {'error': {
                                    'type': 'clickhouse',
                                    'code': ex.code,
                                    'message': error,
                                }}
                            else:
                                result = {'error': {
                                    'type': 'unknown',
                                    'message': error,
                                }}

                    else:
                        status = 429
                        Reason = namedtuple('reason', 'scope name val limit')
                        reasons = [
                            Reason('global', 'concurrent', g_concurr, gcl),
                            Reason('global', 'per-second', g_rate, grl),
                            Reason('project', 'concurrent', p_concurr, pcl),
                            Reason('project', 'per-second', p_rate, prl)
                        ]
                        reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
                        result = {'error': {
                            'type': 'ratelimit',
                            'message': 'rate limit exceeded',
                            'detail': reason and '{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}'.format(r=reason)
                        }}

    stats.update(query_settings)

    if settings.RECORD_QUERIES:
        # send to redis
        state.record_query({
            'request': request.body,
            'sql': sql,
            'timing': timer,
            'stats': stats,
            'status': status,
        })

        timer.send_metrics_to(
            metrics,
            tags={
                'status': str(status),
                'referrer': stats.get('referrer', 'none'),
                'final': str(stats.get('final', False)),
            },
            mark_tags={
                'final': str(stats.get('final', False)),
            }
        )

    result['timing'] = timer

    if settings.STATS_IN_RESPONSE or request.settings.debug:
        result['stats'] = stats
        result['sql'] = sql

    return QueryResult(result, status)
