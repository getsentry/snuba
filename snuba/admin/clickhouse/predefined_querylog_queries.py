from __future__ import annotations

from snuba.admin.clickhouse.common import PreDefinedQuery
from snuba.utils.registered_class import RegisteredClass


class QuerylogQuery(PreDefinedQuery, metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


class QueryByID(QuerylogQuery):
    """Find a query by its ID"""

    sql = """
    SELECT <fields>
    FROM querylog_local
    WHERE has(clickhouse_queries.query_id, '<UUID>')
    AND timestamp > (now() - 60)
    AND timestamp < toDateTime('2022-11-18T19:20:00')

    """


class SQLQueriesForRequestID(QuerylogQuery):
    """Get all the Clickhouse SQL queries for a given request ID"""

    sql = """
    SELECT arrayJoin(clickhouse_queries.sql)
    FROM querylog_local
    WHERE request_id = '<UUID>'
    AND timestamp > (now() - 60)
    AND timestamp < now()
    ORDER BY timestamp ASC
    """


class DurationForReferrerByProject(QuerylogQuery):
    """For a given referrer, show the top 5 projects with the highest cumulative request duration over time. This is used to track changes  in usage by project. If one project appears in the top 5 part way through the timeline, or a project drastically increases part way through the timeline, then that is a signal of abuse by a single customer."""

    sql = """
    SELECT time, projects, c
    FROM (
        SELECT
            toStartOfTenMinutes(timestamp) AS time,
            arrayJoin(projects) as projects,
            sum(duration_ms) AS c
        FROM querylog_local
        WHERE referrer = 'search'
        AND time > (now() - (1 * 3600))
        AND time < now()
        GROUP BY
            projects,
            time
        ORDER BY
            time ASC,
            c DESC
        LIMIT 5 BY time
    )
    ORDER BY projects ASC, time ASC
    """


class BytesScannedForReferrerByProject(QuerylogQuery):
    """For a given referrer, show the top 5 projects with the highest cumulative bytes scanned over time. This is used to track changes in usage by project. If one project appears in the top 5 part way through the timeline, or a project drastically increases part way through the timeline, then that is a signal of abuse by a single customer."""

    sql = """
    SELECT time, projects, c
    FROM (
        SELECT
            toStartOfTenMinutes(timestamp) AS time,
            arrayJoin(projects) as projects,
            sum(arraySum(clickhouse_queries.bytes_scanned)) AS c
        FROM querylog_local
        WHERE referrer = 'search'
        AND time > (now() - (1 * 3600))
        AND time < now()
        GROUP BY
            projects,
            time
        ORDER BY
            time ASC,
            c DESC
        LIMIT 5 BY time
    )
    ORDER BY projects ASC, time ASC
    """


class QueryDurationForReferrerByProject(QuerylogQuery):
    """For a given referrer, show the top 5 projects with the highest cumulative query duration over time. This is used to track changes in usage by project. If one project appears in the top 5 part way through the timeline, or a project drastically increases part way through the timeline, then that is a signal of abuse by a single customer."""

    sql = """
    SELECT time, projects, c
    FROM (
        SELECT
            toStartOfTenMinutes(timestamp) AS time,
            arrayJoin(projects) as projects,
            sum(arraySum(clickhouse_queries.duration_ms)) AS c
        FROM querylog_local
        WHERE referrer = 'search'
        AND time > (now() - (1 * 3600))
        AND time < now()
        GROUP BY
            projects,
            time
        ORDER BY
            time ASC,
            c DESC
        LIMIT 5 BY time
    )
    ORDER BY projects ASC, time ASC
    """


class BeforeAfterBytesScannedComparison(QuerylogQuery):
    """Given a certain time that abuse started on a certain cluster, specify a time range before and after the abuse (recommend 30 minutes). This will show the referrers with the largest change in bytes_scanned. In the example, the abuse started on Dec 7. at 21:54:50."""

    sql = """
    SELECT referrer, before_scanned, after_scanned, (after_scanned - ifNull(before_scanned, 0)) as diff, (diff / if(equals(after_scanned, 0), 1, after_scanned))  * 100 as pct_diff
    FROM
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as after_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('2022-12-07T21:55:00')
        AND timestamp <= toDateTime('2022-12-07T22:20:00')
        AND dataset IN ('events', 'discover')
        GROUP BY referrer
    ) `after` LEFT OUTER JOIN
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as before_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('2022-12-07T21:24:00')
        AND timestamp <= toDateTime('2022-12-07T21:54:00')
        AND dataset IN ('events', 'discover')
        GROUP BY referrer
    ) `before` USING referrer
    ORDER BY pct_diff DESC
    LIMIT 10
    """


class BeforeAfterDurationComparison(QuerylogQuery):
    """Given a certain time that abuse started on a certain cluster, specify a time range before and after the abuse (recommend 30 minutes). This will show the referrers with the largest change in duration. In the example, the abuse started on Dec 7. at 21:54:50."""

    sql = """
    SELECT referrer, before_duration, after_duration, (after_duration - ifNull(before_duration, 0)) as diff, (diff / if(equals(after_duration, 0), 1, after_duration))  * 100 as pct_diff
    FROM
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.duration_ms)) as after_duration
        FROM querylog_local
        WHERE timestamp >= toDateTime('2022-12-07T21:55:00')
        AND timestamp <= toDateTime('2022-12-07T22:20:00')
        AND dataset IN ('events', 'discover')
        GROUP BY referrer
    ) `after` LEFT OUTER JOIN
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.duration_ms)) as before_duration
        FROM querylog_local
        WHERE timestamp >= toDateTime('2022-12-07T21:24:00')
        AND timestamp <= toDateTime('2022-12-07T21:54:00')
        AND dataset IN ('events', 'discover')
        GROUP BY referrer
    ) `before` USING referrer
    ORDER BY pct_diff DESC
    LIMIT 10
    """
