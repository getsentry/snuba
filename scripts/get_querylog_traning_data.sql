SELECT
    toUnixTimestamp(timestamp) AS time,
    sum(duration_ms) AS duration,
    sum(arrayCount(clickhouse_queries.final)) AS num_finall,
    sum(arrayCount(x -> x='invalid_request', clickhouse_queries.status)) as num_invalid_requests,
    sum(arrayCount(x -> x='timeout', clickhouse_queries.status)) as timeouts,
    sum(arraySum(clickhouse_queries.max_threads)) as sum_max_threads,
    sum(arraySum(clickhouse_queries.num_days)) as sum_num_days,
    sum(length(clickhouse_queries.all_columns)) as sum_num_cols,
	sum(arraySum(q -> length(q), clickhouse_queries.sql)) as num_query_chars,
    sum(arraySum(clickhouse_queries.bytes_scanned)) as sum_bytes_scanned
FROM querylog_local
WHERE
status != 'rate_limited'
AND time >= 1675130520
AND time <= 1675131655
GROUP BY
    time
ORDER BY
    time ASC
