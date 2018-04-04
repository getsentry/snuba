import re

TESTING = False
DEBUG = True

# Clickhouse Options
CLICKHOUSE_SERVER = 'localhost:9000'
CLICKHOUSE_TABLE = 'sentry_dist'

# Sentry Options
SENTRY_DSN = 'https://4aa266a7bb2f465aa4a80eca3284b55f:7eb958f65cc743a487b3e319cfc662d8@sentry.io/300688'

# Snuba Options
TIME_GROUPS = {
    3600: 'toStartOfHour(timestamp)',
    60: 'toStartOfMinute(timestamp)',
    86400: 'toDate(timestamp)',
}
DEFAULT_TIME_GROUP = 'toDate(timestamp)'
TIME_GROUP_COLUMN = 'time'

# Processor/Writer Options
DEFAULT_BROKERS = ['localhost:9093']
PROMOTED_TAGS = [
    'level',
    'logger',
    'server_name',
    'transaction',
    'environment',
    'release',
    'dist',
    'site',
    'url',
    'app_device',
    'device',
    'device_family',
    'runtime',
    'runtime_name',
    'browser',
    'browser_name',
    'os',
    'os_name',
    'os_rooted',
]
PROMOTED_CONTEXTS = [
    'os_build',
    'os_kernel_version',
    'device_name',
    'device_brand',
    'device_locale',
    'device_uuid',
    'device_model_id',
    'device_arch',
    'device_battery_level',
    'device_orientation',
    'device_simulator',
    'device_online',
    'device_charging'
]
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
] + PROMOTED_CONTEXTS + PROMOTED_TAGS + [
    'tags.key',
    'tags.value',
    'contexts.key',
    'contexts.value',
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

# A column name like "tags[url]"
NESTED_COL_EXPR = re.compile('^(tags)\[([a-zA-Z0-9_\.:-]+)\]$')

# The set of columns, and associated keys that have been promoted
# to the top level table namespace
PROMOTED_COLS = {
    'tags': PROMOTED_TAGS,
    'contexts': PROMOTED_CONTEXTS,
}


# Table Definitions
SCHEMA_COLUMNS = """
    -- required and provided by SDK
    event_id FixedString(32),
    timestamp DateTime,
    platform String,
    message String,

    -- required and provided by Sentry
    primary_hash FixedString(32),
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

    -- contexts
    os_build Nullable(String),
    os_kernel_version Nullable(String),
    device_name Nullable(String),
    device_brand Nullable(String),
    device_locale Nullable(String),
    device_uuid Nullable(String),
    device_model Nullable(String),
    device_model_id Nullable(String),
    device_arch Nullable(String),
    device_battery_level Nullable(Float32),
    device_orientation Nullable(String),
    device_simulator Nullable(UInt8),
    device_online Nullable(UInt8),
    device_charging Nullable(UInt8),

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
    app_device Nullable(String),
    device Nullable(String),
    device_family Nullable(String),
    runtime Nullable(String),
    runtime_name Nullable(String),
    browser Nullable(String),
    browser_name Nullable(String),
    os Nullable(String),
    os_name Nullable(String),
    os_rooted Nullable(UInt8),

    -- other tags
    tags Nested (
        key String,
        value String
    ),

    -- other context
    contexts Nested (
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
        stack_level UInt16
    )
"""

DEFAULT_ORDER_BY = '(project_id, timestamp)'
DEFAULT_PARTITION_BY = '(toStartOfDay(timestamp))'  # modulo(intHash32(project_id), 32)
DEFAULT_SHARDING_KEY = 'rand()'

DEFAULT_CLICKHOUSE_NODES = [
    'clickhouse-08b7387d',
    'clickhouse-a8ef8458',
    'clickhouse-649c2398',
    'clickhouse-f8e2348b'
]
DEFAULT_LOCAL_TABLE = 'sentry_local'
DEFAULT_DIST_TABLE = 'sentry_dist'
