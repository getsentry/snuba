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


class MostThrottledOrgs(QuerylogQuery):
    """Orgs with the highest ratios of throttled queries. This isn't perfect, it just shows how many queries an Allocation Policy has set to
    not 10 max threads (ie throttled to 1 thread) for some reason. How many threads ClickHouse would've run the query with given max 10 threads is still unknown."""

    sql = """
    SELECT organization, throttled_queries, total_queries, divide(throttled_queries, total_queries) as ratio
    FROM
    (
        SELECT organization, count(*) as throttled_queries
        FROM querylog_local
        WHERE
            timestamp > (now() - {{duration}})
            AND JSONExtractRaw(JSONExtractRaw(arrayJoin(clickhouse_queries.stats), 'quota_allowance'), 'explanation') != '{}'
            AND JSONExtractInt(JSONExtractRaw(arrayJoin(clickhouse_queries.stats), 'quota_allowance'), 'max_threads') != 10
            AND timestamp < now()
        GROUP BY organization
    )
    AS throttled_orgs
    INNER JOIN
    (
        SELECT organization, count(*) as total_queries
        FROM querylog_local
        WHERE
            timestamp > (now() - {{duration}})
            AND arrayJoin(clickhouse_queries.query_id) != 'bad_id_xyz'
            AND timestamp < now()
        GROUP BY organization
    ) AS queries_by_org
    ON throttled_orgs.organization = queries_by_org.organization
    ORDER BY ratio desc
    LIMIT 20
    """


class OrgQueryDurationQuantiles(QuerylogQuery):
    """Returns count, p50, p75, p90, p99 of all queries per Organization over a certain period of time, sorted by descending p99"""

    sql = """
    SELECT
        organization,
        sum(c) as total_queries,
        quantile(0.50)(duration_ms) as p50,
        quantile(0.75)(duration_ms) as p75,
        quantile(0.9)(duration_ms) as p90,
        quantile(0.99)(duration_ms) as p99
    FROM
    (
        SELECT organization, count(*) as c, arrayJoin(clickhouse_queries.duration_ms) as duration_ms
        FROM querylog_local
        WHERE
            timestamp < now()
            AND arrayJoin(clickhouse_queries.query_id) != 'bad_id_xyz'
            AND timestamp > (now() - {{duration}})
        GROUP BY organization, duration_ms
    )
    GROUP BY organization
    ORDER BY p99 DESC
    LIMIT 20
"""


class TotalBytesScannedByReferrerAndProject(QuerylogQuery):
    """
    Returns the list of top 10 projects causing the most bytes scanned for a specific referrer for a given time window.
    """

    sql = """
    SELECT
        arrayJoin(projects) as projects,
        formatReadableSize(sum(arraySum(clickhouse_queries.bytes_scanned)) AS total_bytes) AS bytes_scanned
    FROM querylog_dist
    WHERE referrer = '{{referrer}}'
    AND timestamp > (now() - ({{duration}}))
    AND timestamp < now()
    GROUP BY
        projects
    ORDER BY
       total_bytes DESC
    LIMIT 10
"""
