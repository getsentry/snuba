import json
import random

from clickhouse_driver import Client
from datetime import datetime
from kafka import KafkaConsumer


# TODO: schema changes:
#   * message params -> string array
#   * span_id -> uuid
#   * transaction_id -> uuid
#   * context
#       store all well known fields https://docs.sentry.io/clientdev/interfaces/contexts/
#       dedupe/promote from two places:
#           tags
#           UserAgentPlugin model(s) in Sentry codebase


CLICKHOUSE_NODES = [
    'clickhouse-08b7387d',
    'clickhouse-a8ef8458',
    'clickhouse-649c2398',
    'clickhouse-f8e2348b'
]
KAFKA_TOPIC = 'events'
KAFKA_BROKERS = ['localhost:9093']
KAFKA_CONSUMER_GROUP = 'snuba'
LOCAL_TABLE = 'sentry_local'
DIST_TABLE = 'sentry_dist'
CLUSTER = 'cluster1'
DATABASE = 'default'
BATCH_SIZE = 5000
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

MAX_UINT32 = 2 * 32 - 1


def _collapse_uint32(n):
    if (n is None) or (n < 0) or (n > MAX_UINT32):
        return None
    return n

def _unicodify(s):
    if not s:
        return None

    if isinstance(s, dict) or isinstance(s, list):
        return json.dumps(s)

    return unicode(s)


batch = []
connections = [Client(node) for node in CLICKHOUSE_NODES]
kafka = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    group_id=KAFKA_CONSUMER_GROUP,
)

for conn in connections:
    conn.execute(LOCAL_TABLE_DEFINITION)
    conn.execute(DIST_TABLE_DEFINITION)


for msg in kafka:
    msg = json.loads(msg.value)

    # TODO: remove _unicodify, splice, and rjust once we stop sending Postgres integer ids
    event_id = _unicodify(msg['event_id'])
    event_id = event_id[-32:].rjust(32) if event_id else ('0' * 32)

    # TODO: remove splice and rjust once we handle 'checksum' hashes (which are too long)
    primary_hash = msg['primary_hash'][-16:].rjust(16)

    project_id = msg['project_id']
    message = _unicodify(msg['message'])
    platform = _unicodify(msg['platform'])
    timestamp = datetime.strptime(msg['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ")

    data = msg.get('data', {})

    received = datetime.fromtimestamp(data['received'])

    sdk = data.get('sdk', {})
    sdk_name = _unicodify(sdk.get('name', None))
    sdk_version = _unicodify(sdk.get('version', None))

    tags = dict(data.get('tags', []))

    tags.pop('sentry:user', None) # defer to user interface data (below)
    level = _unicodify(tags.pop('level', None))
    logger = _unicodify(tags.pop('logger', None))
    server_name = _unicodify(tags.pop('server_name', None))
    transaction = _unicodify(tags.pop('transaction', None))
    environment = _unicodify(tags.pop('environment', None))
    release = _unicodify(tags.pop('sentry:release', None))
    dist = _unicodify(tags.pop('sentry:dist', None))
    site = _unicodify(tags.pop('site', None))
    url = _unicodify(tags.pop('url', None))

    user = data.get('sentry.interfaces.User', {})
    user_id = _unicodify(user.get('id', None))
    username = _unicodify(user.get('username', None))
    email = _unicodify(user.get('email', None))
    ip_address = _unicodify(user.get('ip_address', None))

    http = data.get('sentry.interfaces.Http', {})
    http_method = _unicodify(http.get('method', None))

    http_headers = dict(http.get('headers', []))
    http_referer = _unicodify(http_headers.get('Referer', None))

    tag_keys = []
    tag_values = []
    for tag_key, tag_value in tags.items():
        tag_keys.append(_unicodify(tag_key))
        tag_values.append(_unicodify(tag_value))

    stack_types = []
    stack_values = []

    frame_abs_paths = []
    frame_filenames = []
    frame_packages = []
    frame_modules = []
    frame_functions = []
    frame_in_app = []
    frame_colnos = []
    frame_linenos = []
    frame_stack_levels = []

    stack_level = 0
    stacks = data.get('sentry.interfaces.Exception', {}).get('values', [])
    for stack in stacks[:200]:
        stack_types.append(_unicodify(stack.get('type', None)))
        stack_values.append(_unicodify(stack.get('value', None)))

        frames = stack.get('stacktrace', {}).get('frames', [])
        for frame in frames:
            frame_abs_paths.append(_unicodify(frame.get('abs_path', None)))
            frame_filenames.append(_unicodify(frame.get('filename', None)))
            frame_packages.append(_unicodify(frame.get('package', None)))
            frame_modules.append(_unicodify(frame.get('module', None)))
            frame_functions.append(_unicodify(frame.get('function', None)))
            frame_in_app.append(frame.get('in_app', None))
            frame_colnos.append(_collapse_uint32(frame.get('colno', None)))
            frame_linenos.append(_collapse_uint32(frame.get('lineno', None)))
            frame_stack_levels.append(stack_level)

        stack_level += 1

    batch.append(
        (event_id,
         timestamp,
         platform,
         message,
         primary_hash,
         project_id,
         received,
         user_id,
         username,
         email,
         ip_address,
         sdk_name,
         sdk_version,
         level,
         logger,
         server_name,
         transaction,
         environment,
         release,
         dist,
         site,
         url,
         tag_keys,
         tag_values,
         http_method,
         http_referer,
         stack_types,
         stack_values,
         frame_abs_paths,
         frame_filenames,
         frame_packages,
         frame_modules,
         frame_functions,
         frame_in_app,
         frame_colnos,
         frame_linenos,
         frame_stack_levels
         ))

    if (len(batch) >= BATCH_SIZE):
        random.choice(connections).execute("""
        INSERT INTO %(table)s (
            event_id,
            timestamp,
            platform,
            message,
            primary_hash,
            project_id,
            received,
            user_id,
            username,
            email,
            ip_address,
            sdk_name,
            sdk_version,
            level,
            logger,
            server_name,
            transaction,
            environment,
            release,
            dist,
            site,
            url,
            tags.key,
            tags.value,
            http_method,
            http_referer,
            exception_stacks.type,
            exception_stacks.value,
            exception_frames.abs_path,
            exception_frames.filename,
            exception_frames.package,
            exception_frames.module,
            exception_frames.function,
            exception_frames.in_app,
            exception_frames.colno,
            exception_frames.lineno,
            exception_frames.stack_level
        ) VALUES
        """ % {'table': DIST_TABLE}, batch)
        batch = []
