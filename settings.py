# Clickhouse Options
CLICKHOUSE_SERVER = 'http://localhost:8123'
CLICKHOUSE_TABLE = 'sentry_dist'

# Sentry Options
SENTRY_DSN = 'https://4aa266a7bb2f465aa4a80eca3284b55f:7eb958f65cc743a487b3e319cfc662d8@sentry.io/300688'

# Snuba Options
AGGREGATE_RESULT_COLUMN = 'aggregate'
TIME_GROUPS = {
    3600: 'toStartOfHour(timestamp)',
    60: 'toStartOfMinute(timestamp)',
    86400: 'toDate(timestamp)',
}
DEFAULT_TIME_GROUP = 'toDate(timestamp)'
TIME_GROUP_COLUMN = 'time'
