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

# Processor/Writer Options
CLICKHOUSE_NODES = [
    'clickhouse-08b7387d',
    'clickhouse-a8ef8458',
    'clickhouse-649c2398',
    'clickhouse-f8e2348b'
]
CLUSTER = 'cluster1'
DATABASE = 'default'
BROKERS = ['localhost:9093']
WRITER_TOPIC = 'snuba'
EVENT_TOPIC = 'events'
BROKERS = ['localhost:9093']
WRITER_CONSUMER_GROUP = 'snuba-writers'
PROCESSOR_CONSUMER_GROUP = 'snuba-processors'
WRITER_BATCH_SIZE = 10000
WRITER_COLUMNS = [
    'event_id',
    'timestamp',
    'platform',
    'message',
    'primary_hash',
    'project_id',
    'received',
    'user_id',
    'username',
    'email',
    'ip_address',
    'sdk_name',
    'sdk_version',
    'level',
    'logger',
    'server_name',
    'transaction',
    'environment',
    'release',
    'dist',
    'site',
    'url',
    'tags.key',
    'tags.value',
    'http_method',
    'http_referer',
    'exception_stacks.type',
    'exception_stacks.value',
    'exception_frames.abs_path',
    'exception_frames.filename',
    'exception_frames.package',
    'exception_frames.module',
    'exception_frames.function',
    'exception_frames.in_app',
    'exception_frames.colno',
    'exception_frames.lineno',
    'exception_frames.stack_level',
]

# Table Definitions
LOCAL_TABLE = 'sentry_local'
DIST_TABLE = 'sentry_dist'

COLUMNS = """
    -- required and provided by SDK
    event_id FixedString(32),
    timestamp DateTime,
    platform String,
    message String,

    -- required and provided by Sentry
    primary_hash FixedString(16),
    project_id UInt64,
    received DateTime,

    -- optional user
    user_id Nullable(String),
    username Nullable(String),
    email Nullable(String),
    ip_address Nullable(String),

    -- optional misc
    sdk_name Nullable(String),
    sdk_version Nullable(String),

    -- promoted tags
    level Nullable(String),
    logger Nullable(String),
    server_name Nullable(String), -- future name: device_id?
    transaction Nullable(String),
    environment Nullable(String),
    release Nullable(String), -- sentry:release
    dist Nullable(String), -- sentry:dist
    site Nullable(String),
    url Nullable(String),

    -- other tags
    tags Nested (
        key String,
        value String
    ),

    -- interfaces

    -- http interface
    http_method Nullable(String),
    http_referer Nullable(String),

    -- exception interface
    exception_stacks Nested (
        type Nullable(String),
        value Nullable(String)
    ),
    exception_frames Nested (
        abs_path Nullable(String),
        filename Nullable(String),
        package Nullable(String),
        module Nullable(String),
        function Nullable(String),
        in_app Nullable(UInt8),
        colno Nullable(UInt32),
        lineno Nullable(UInt32),
        stack_level UInt8
    )
"""

PARTITION_BY = '(toMonday(timestamp), modulo(intHash32(project_id), 32))'
ORDER_BY = '(project_id, timestamp)'
LOCAL_TABLE_DEFINITION = """
CREATE TABLE IF NOT EXISTS %(name)s (
    %(columns)s
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/%(name)s',
    '{replica}'
) PARTITION BY %(partition_by)s
  ORDER BY %(order_by)s;""" % {
    'columns': COLUMNS,
    'name': LOCAL_TABLE,
    'order_by': ORDER_BY,
    'partition_by': PARTITION_BY,
}

DIST_TABLE_DEFINITION = """
CREATE TABLE IF NOT EXISTS %(name)s (
    %(columns)s
) ENGINE = Distributed(
    %(cluster)s,
    %(database)s,
    %(local_table)s,
    %(sharding_key)s
);""" % {
    'cluster': CLUSTER,
    'columns': COLUMNS,
    'database': DATABASE,
    'local_table': LOCAL_TABLE,
    'name': DIST_TABLE,
    'sharding_key': 'rand()',
}
