import json
import time

from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer


# TODO: schema changes:
#   * message params -> string array
#   * span_id -> uuid
#   * transaction_id -> uuid
#   * context
#       store all well known fields https://docs.sentry.io/clientdev/interfaces/contexts/
#       dedupe/promote from two places:
#           tags
#           UserAgentPlugin model(s) in Sentry codebase


EVENT_TOPIC = 'events'
WRITER_TOPIC = 'snuba'
BROKERS = ['localhost:9093']
CONSUMER_GROUP = 'snuba-processors'
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

consumer = KafkaConsumer(
    EVENT_TOPIC,
    bootstrap_servers=BROKERS,
    group_id=CONSUMER_GROUP,
)

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    linger_ms=50,
)


def process_event(event):
    # TODO: remove _unicodify, splice, and rjust once we stop sending Postgres integer ids
    event_id = _unicodify(event['event_id'])
    event_id = event_id[-32:].rjust(32) if event_id else ('0' * 32)

    # TODO: remove splice and rjust once we handle 'checksum' hashes (which are too long)
    primary_hash = event['primary_hash'][-16:].rjust(16)

    project_id = event['project_id']
    message = _unicodify(event['message'])
    platform = _unicodify(event['platform'])
    timestamp = time.mktime(datetime.strptime(event['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())

    data = event.get('data', {})

    received = data['received']

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

    row = (
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
    )

    key = '%s:%s' % (event_id, project_id)
    producer.send(WRITER_TOPIC, key=key.encode('utf-8'), value=json.dumps(row).encode('utf-8'))


for msg in consumer:
    process_event(json.loads(msg.value))

