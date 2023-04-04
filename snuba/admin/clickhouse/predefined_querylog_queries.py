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
    SELECT {{fields}}
    FROM querylog_local
    WHERE has(clickhouse_queries.query_id, '{{UUID}}')
    AND timestamp > (now() - 60)
    AND timestamp < toDateTime('{{time}}')

    """


class SQLQueriesForRequestID(QuerylogQuery):
    """Get all the Clickhouse SQL queries for a given request ID"""

    sql = """
    SELECT arrayJoin(clickhouse_queries.sql)
    FROM querylog_local
    WHERE request_id = '{{UUID}}'
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
        WHERE referrer = '{{referrer}}'
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
        WHERE referrer = '{{referrer}}'
        AND time > (now() - ({{duration}}))
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
        WHERE referrer = '{{referrer}}'
        AND time > (now() - ({{duration}}))
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
    """Given a certain time that abuse started on a certain cluster, specify a time range before and after the abuse (recommend 30 minutes). This will show the referrers with the largest change in bytes_scanned."""

    sql = """
    SELECT referrer, before_scanned, after_scanned, (after_scanned - ifNull(before_scanned, 0)) as diff, (diff / if(equals(after_scanned, 0), 1, after_scanned))  * 100 as pct_diff
    FROM
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as after_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{{after_scanned_duration_start}}')
        AND timestamp <= toDateTime('{{after_scanned_duration_end}}')
        AND dataset IN ('{{dataset}}')
        GROUP BY referrer
    ) `after` LEFT OUTER JOIN
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as before_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{{before_scanned_duration_start}}')
        AND timestamp <= toDateTime('{{before_scanned_duration_end}}')
        AND dataset IN ('{{dataset}}')
        GROUP BY referrer
    ) `before` USING referrer
    ORDER BY pct_diff DESC
    LIMIT 10
    """


class BeforeAfterDurationComparison(QuerylogQuery):
    """Given a certain time that abuse started on a certain cluster, specify a time range before and after the abuse (recommend 30 minutes). This will show the referrers with the largest change in duration."""

    sql = """
    SELECT referrer, before_duration, after_duration, (after_duration - ifNull(before_duration, 0)) as diff, (diff / if(equals(after_duration, 0), 1, after_duration))  * 100 as pct_diff
    FROM
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.duration_ms)) as after_duration
        FROM querylog_local
        WHERE timestamp >= toDateTime('{{after_duration_start}}')
        AND timestamp <= toDateTime('{{after_duration_end}}')
        AND dataset IN ('{{dataset}}')
        GROUP BY referrer
    ) `after` LEFT OUTER JOIN
    (
        SELECT referrer, sum(arrayReduce('sum', clickhouse_queries.duration_ms)) as before_duration
        FROM querylog_local
        WHERE timestamp >= toDateTime('{{before_duartion_start}}')
        AND timestamp <= toDateTime('{{before_duartion_end}}')
        AND dataset IN ('{{dataset}}')
        GROUP BY referrer
    ) `before` USING referrer
    ORDER BY pct_diff DESC
    LIMIT 10
    """


class TopNReferrerBytotalByteScannedPercentage(QuerylogQuery):
    """Specify a time period and N. Get the top N referrers with the highest percentage of all bytes scanned."""

    sql = """
    WITH
    (
        SELECT sum(arrayReduce('sum', clickhouse_queries.bytes_scanned))
        FROM querylog_local
        WHERE (timestamp > ({{from}})) AND (timestamp < {{to}})
        AND dataset IN ('{{dataset}}')
    ) AS all_bytes_scanned
    SELECT
        referrer,
        sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) AS bytes_scanned,
        all_bytes_scanned,
        round((bytes_scanned / all_bytes_scanned) * 100, 3) AS pct
    FROM querylog_local
    WHERE (timestamp > ({{from}})) AND (timestamp < {{to}})
    AND dataset IN ('{{dataset}}')
    GROUP BY referrer
    ORDER BY bytes_scanned DESC
    LIMIT {{n}}
    """


class ChangeInTotalByteScannedPercentage(QuerylogQuery):
    """Specify a 2 time periods and an org ID. Get the change in percentage of all bytes scanned that org used between those time periods."""

    sql = """
    WITH
    (
        SELECT sum(arrayReduce('sum', clickhouse_queries.bytes_scanned))
        FROM querylog_local
        WHERE (timestamp > ({{t0}}))
        AND (timestamp < {{t0}} + {{delta_t}})
        AND dataset IN ('{{dataset}}')
    ) AS all_bytes_scanned_p0,
    (
        SELECT sum(arrayReduce('sum', clickhouse_queries.bytes_scanned))
        FROM querylog_local
        WHERE (timestamp > ({{t1}}))
        AND (timestamp < {{t1}} + {{delta_t}})
        AND dataset IN ('{{dataset}}')
    ) AS all_bytes_scanned_p1,
    (
        SELECT sum(arrayReduce('sum', clickhouse_queries.bytes_scanned))
        FROM querylog_local
        WHERE organization = {{org}}
        AND (timestamp > ({{t0}}))
        AND (timestamp < {{t0}} + {{delta_t}})
        AND dataset IN ('{{dataset}}')
    ) AS bytes_scanned_p0,
    (
        SELECT sum(arrayReduce('sum', clickhouse_queries.bytes_scanned))
        FROM querylog_local
        WHERE organization = {{org}}
        AND (timestamp > ({{t1}}))
        AND (timestamp < {{t1}} + {{delta_t}})
        AND dataset IN ('{{dataset}}')
    ) AS bytes_scanned_p1
    SELECT
        bytes_scanned_p0,
        all_bytes_scanned_p0,
        round(bytes_scanned_p0 / all_bytes_scanned_p0 * 100, 4) AS pct_bytes_scanned_p0,
        bytes_scanned_p1,
        all_bytes_scanned_p1,
        round(bytes_scanned_p1 / all_bytes_scanned_p1 * 100, 4) AS pct_bytes_scanned_p1,
        round(pct_bytes_scanned_p1 - pct_bytes_scanned_p0, 4) AS delta_pct
    """


class DurationForReferrerByOrganizations(QuerylogQuery):
    """For a given referrer, show the top organizations with the highest cumulative request duration over time."""

    sql = """
    SELECT time, organization, c
    FROM (
        SELECT
            toStartOfTenMinutes(timestamp) AS time,
            sum(duration_ms) AS c,
            organization
        FROM querylog_local
        WHERE referrer = '{{referrer}}'
        AND time > (now() - ({{duration}}))
        AND time < now()
        GROUP BY
            organization,
            time
        ORDER BY
            time ASC,
            c DESC
        LIMIT 5 BY time
    )
    ORDER BY c DESC
    """


class BytesScannedForReferrerByOrganization(QuerylogQuery):
    """For a given referrer, show the top organizations with the highest cumulative bytes scanned over time. Duration is how many seconds ago to begin."""

    sql = """
    SELECT time, organization, c
    FROM (
        SELECT
            toStartOfTenMinutes(timestamp) AS time,
            sum(arraySum(clickhouse_queries.bytes_scanned)) AS c,
            organization
        FROM querylog_local
        WHERE referrer = '{{referrer}}'
        AND time > (now() - ({{duration}}))
        AND time < now()
        GROUP BY
            organization,
            time
        ORDER BY
            time ASC,
            c DESC
        LIMIT 5 BY time
    )
    ORDER BY c DESC
    """
