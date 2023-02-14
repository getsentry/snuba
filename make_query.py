from datetime import datetime, timezone

q = """
    SELECT project_id, before_scanned, after_scanned, (after_scanned - ifNull(before_scanned, 0)) as diff
    FROM
    (
        SELECT arrayJoin(projects) AS project_id, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as after_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{abuse_start_date}')
        AND timestamp <= toDateTime('{abuse_end_date}')
        AND dataset IN ('events', 'discover')
        AND referrer = '{referrer}'
        GROUP BY project_id
    ) `after` LEFT OUTER JOIN
    (
        SELECT arrayJoin(projects) AS project_id, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as before_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{abuse_start_date}') - toIntervalDay(7)
        AND timestamp <= toDateTime('{abuse_end_date}') - toIntervalDay(7)
        AND dataset IN ('events', 'discover')
        AND referrer = '{referrer}'
        GROUP BY project_id
    ) `before` USING project_id
    ORDER BY diff DESC
    LIMIT 10
"""


q_avg = """
SELECT avg(after_scanned - ifNull(before_scanned, 0)) as avg_increase, stddevPop(after_scanned - ifNull(before_scanned, 0)) AS std_dev_increase, max(after_scanned - ifNull(before_scanned, 0)) as max_increase
    FROM
    (
        SELECT arrayJoin(projects) AS project_id, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as after_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{abuse_start_date}')
        AND timestamp <= toDateTime('{abuse_end_date}')
        AND dataset IN ('events', 'discover')
        AND referrer = '{referrer}'
        GROUP BY project_id
    ) `after` LEFT OUTER JOIN
    (
        SELECT arrayJoin(projects) AS project_id, sum(arrayReduce('sum', clickhouse_queries.bytes_scanned)) as before_scanned
        FROM querylog_local
        WHERE timestamp >= toDateTime('{abuse_start_date}') - toIntervalDay(7)
        AND timestamp <= toDateTime('{abuse_end_date}') - toIntervalDay(7)
        AND dataset IN ('events', 'discover')
        AND referrer = '{referrer}'
        GROUP BY project_id
    ) `before` USING project_id
"""

# this query is not right
q_referrer_usage = """
    SELECT
        arrayJoin(projects) as projects,
        sum(arraySum(clickhouse_queries.bytes_scanned)) AS c
    FROM querylog_local
    WHERE referrer = '{referrer}'
    AND timestamp > toDateTime('{abuse_start_date}')
    AND timestamp < toDateTime('{abuse_end_date}')
    GROUP BY
        projects
    ORDER BY
        c DESC
    LIMIT 10

"""

q_feature_matrix = """
SELECT
    toUnixTimestamp(toStartOfMinute(timestamp)) AS time,
    sum(duration_ms) AS duration,
    sum(arrayCount(clickhouse_queries.final)) AS num_finall,
    sum(arrayCount(x -> x='invalid_request', clickhouse_queries.status)) as num_invalid_requests,
    sum(arrayCount(x -> x='timeout', clickhouse_queries.status)) as timeouts,
    sum(arraySum(clickhouse_queries.max_threads)) as sum_max_threads,
    sum(arraySum(clickhouse_queries.num_days)) as sum_num_days,
    sum(length(clickhouse_queries.all_columns)) as sum_num_cols,
    sum(arraySum(clickhouse_queries.bytes_scanned)) as sum_bytes_scanned
FROM querylog_local
WHERE
status != 'rate_limited'
AND time > toDateTime('{abuse_start_date}')
AND time < toDateTime('{abuse_end_date}')
GROUP BY
    time
ORDER BY
    time ASC
"""


print(
    q_feature_matrix.format(
        abuse_start_date=datetime(2023, 1, 31, 2, 7, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        abuse_end_date=datetime(2023, 1, 31, 2, 16, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
    )
)
