import re
import os
from collections import defaultdict


class dynamicdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory:
            self.__setitem__(key, self.default_factory(key))
            return self[key]
        else:
            return super(dynamicdict, self).__missing__(key)


LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

TESTING = False
DEBUG = True

PORT = 1218

# Clickhouse Options
CLICKHOUSE_SERVER = os.environ.get('CLICKHOUSE_SERVER', 'localhost:9000')
CLICKHOUSE_CLUSTER = None
CLICKHOUSE_TABLE = 'dev'
CLICKHOUSE_MAX_POOL_SIZE = 25

# Dogstatsd Options
DOGSTATSD_HOST = 'localhost'
DOGSTATSD_PORT = 8125

# Redis Options
USE_REDIS_CLUSTER = False
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 1

# Query Recording Options
RECORD_QUERIES = False
QUERIES_TOPIC = 'snuba-queries'

# Runtime Config Options
CONFIG_MEMOIZE_TIMEOUT = 10

# Sentry Options
SENTRY_DSN = None

# Snuba Options
TIME_GROUPS = dynamicdict(
    lambda sec: 'toDateTime(intDiv(toUInt32(timestamp), {0}) * {0})'.format(sec),
    {
        3600: 'toStartOfHour(timestamp)',
        60: 'toStartOfMinute(timestamp)',
        86400: 'toDate(timestamp)',
    }
)

TIME_GROUP_COLUMN = 'time'

# Processor/Writer Options
DEFAULT_BROKERS = ['localhost:9093']
DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 20000
# Columns that come from outside the event body itself
METADATA_COLUMNS = [
    'offset',
    'partition',
]
PROMOTED_TAG_COLUMNS = [
    # These are the classic tags, they are saved in Snuba exactly as they
    # appear in the event body.
    'level',
    'logger',
    'server_name',
    'transaction',
    'environment',
    'sentry:release',
    'sentry:dist',
    'sentry:user',
    'site',
    'url',
]
PROMOTED_CONTEXT_TAG_COLUMNS = [
    # These are promoted tags that come in in `tags`, but are more closely
    # related to contexts.  To avoid naming confusion with Clickhouse nested
    # columns, they are stored in the database with s/./_/
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
PROMOTED_CONTEXT_COLUMNS = [
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
REQUIRED_COLUMNS = [
    'event_id',
    'project_id',
    'group_id',
    'timestamp',
    'deleted',
    'retention_days',
]
WRITER_COLUMNS = REQUIRED_COLUMNS + [
    'platform',
    'message',
    'primary_hash',
    'received',
    'user_id',
    'username',
    'email',
    'ip_address',
    'geo_country_code',
    'geo_region',
    'geo_city',
    'sdk_name',
    'sdk_version',
    'type',
    'version',
] + METADATA_COLUMNS + PROMOTED_CONTEXT_COLUMNS + PROMOTED_TAG_COLUMNS + PROMOTED_CONTEXT_TAG_COLUMNS + [
    'tags.key',
    'tags.value',
    'contexts.key',
    'contexts.value',
    'http_method',
    'http_referer',
    'exception_stacks.type',
    'exception_stacks.value',
    'exception_stacks.mechanism_type',
    'exception_stacks.mechanism_handled',
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
NESTED_COL_EXPR = re.compile('^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')

# The set of columns, and associated keys that have been promoted
# to the top level table namespace.
PROMOTED_COLS = {
    'tags': frozenset(PROMOTED_TAG_COLUMNS + PROMOTED_CONTEXT_TAG_COLUMNS),
    'contexts': frozenset(PROMOTED_CONTEXT_COLUMNS),
}

# For every applicable promoted column,  a map of translations from the column
# name  we save in the database to the tag we receive in the query.
COLUMN_TAG_MAP = {
    'tags': {t: t.replace('_', '.') for t in PROMOTED_CONTEXT_TAG_COLUMNS},
    'contexts': {}
}

# And a reverse map from the tags the client expects to the database columns
TAG_COLUMN_MAP = {
    col: dict(map(reversed, trans.items())) for col, trans in COLUMN_TAG_MAP.items()
}

# The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
# and they can/will use a promoted column.
PROMOTED_TAGS = {
    col: [COLUMN_TAG_MAP[col].get(x, x) for x in PROMOTED_COLS[col]]
    for col in PROMOTED_COLS
}

# Column Definitions (Name, Type)
SCHEMA_COLUMNS = [
    # required
    ('event_id', 'FixedString(32)'),
    ('project_id', 'UInt64'),
    ('group_id', 'UInt64'),
    ('timestamp', 'DateTime'),
    ('deleted', 'UInt8'),
    ('retention_days', 'UInt16'),

    # required for non-deleted
    ('platform', 'Nullable(String)'),
    ('message', 'Nullable(String)'),
    ('primary_hash', 'Nullable(FixedString(32))'),
    ('received', 'Nullable(DateTime)'),

    # optional user
    ('user_id', 'Nullable(String)'),
    ('username', 'Nullable(String)'),
    ('email', 'Nullable(String)'),
    ('ip_address', 'Nullable(String)'),

    # optional geo
    ('geo_country_code', 'Nullable(String)'),
    ('geo_region', 'Nullable(String)'),
    ('geo_city', 'Nullable(String)'),

    # optional misc
    ('sdk_name', 'Nullable(String)'),
    ('sdk_version', 'Nullable(String)'),
    ('type', 'Nullable(String)'),
    ('version', 'Nullable(String)'),

    # optional stream related data
    ('offset', 'Nullable(UInt64)'),
    ('partition', 'Nullable(UInt16)'),

    # contexts
    ('os_build', 'Nullable(String)'),
    ('os_kernel_version', 'Nullable(String)'),
    ('device_name', 'Nullable(String)'),
    ('device_brand', 'Nullable(String)'),
    ('device_locale', 'Nullable(String)'),
    ('device_uuid', 'Nullable(String)'),
    ('device_model', 'Nullable(String)'),
    ('device_model_id', 'Nullable(String)'),
    ('device_arch', 'Nullable(String)'),
    ('device_battery_level', 'Nullable(Float32)'),
    ('device_orientation', 'Nullable(String)'),
    ('device_simulator', 'Nullable(UInt8)'),
    ('device_online', 'Nullable(UInt8)'),
    ('device_charging', 'Nullable(UInt8)'),

    # promoted tags
    ('level', 'Nullable(String)'),
    ('logger', 'Nullable(String)'),
    ('server_name', 'Nullable(String)'),  # future name: device_id?
    ('transaction', 'Nullable(String)'),
    ('environment', 'Nullable(String)'),
    ('sentry:release', 'Nullable(String)'),
    ('sentry:dist', 'Nullable(String)'),
    ('sentry:user', 'Nullable(String)'),
    ('site', 'Nullable(String)'),
    ('url', 'Nullable(String)'),
    ('app_device', 'Nullable(String)'),
    ('device', 'Nullable(String)'),
    ('device_family', 'Nullable(String)'),
    ('runtime', 'Nullable(String)'),
    ('runtime_name', 'Nullable(String)'),
    ('browser', 'Nullable(String)'),
    ('browser_name', 'Nullable(String)'),
    ('os', 'Nullable(String)'),
    ('os_name', 'Nullable(String)'),
    ('os_rooted', 'Nullable(UInt8)'),

    # other tags
    ('tags', '''Nested (
        key String,
        value String
    )'''),

    # other context
    ('contexts', '''Nested (
        key String,
        value String
    )'''),

    # interfaces

    # http interface
    ('http_method', 'Nullable(String)'),
    ('http_referer', 'Nullable(String)'),

    # exception interface
    ('exception_stacks', '''Nested (
        type Nullable(String),
        value Nullable(String),
        mechanism_type Nullable(String),
        mechanism_handled Nullable(UInt8)
    )'''),
    ('exception_frames', '''Nested (
        abs_path Nullable(String),
        filename Nullable(String),
        package Nullable(String),
        module Nullable(String),
        function Nullable(String),
        in_app Nullable(UInt8),
        colno Nullable(UInt32),
        lineno Nullable(UInt32),
        stack_level UInt16
    )'''),
]

SCHEMA_MAP = dict(SCHEMA_COLUMNS)

# project_id and timestamp are included for queries, event_id is included for ReplacingMergeTree
DEFAULT_SAMPLE_EXPR = 'cityHash64(toString(event_id))'
DEFAULT_ORDER_BY = '(project_id, toStartOfDay(timestamp), %s)' % DEFAULT_SAMPLE_EXPR
DEFAULT_PARTITION_BY = '(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))'
DEFAULT_VERSION_COLUMN = 'deleted'
DEFAULT_SHARDING_KEY = 'cityHash64(toString(event_id))'
DEFAULT_LOCAL_TABLE = 'sentry_local'
DEFAULT_DIST_TABLE = 'sentry_dist'
DEFAULT_RETENTION_DAYS = 90

RETENTION_OVERRIDES = {}

# the list of keys that will upgrade from a WHERE condition to a PREWHERE
PREWHERE_KEYS = ['project_id']

STATS_IN_RESPONSE = False

PAYLOAD_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
