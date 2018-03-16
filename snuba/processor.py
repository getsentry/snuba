import json
import time

from datetime import datetime


# TODO: schema changes:
#   * message params -> string array
#   * span_id -> uuid
#   * transaction_id -> uuid
#   * context
#       store all well known fields https://docs.sentry.io/clientdev/interfaces/contexts/
#       dedupe/promote from two places:
#           tags
#           UserAgentPlugin model(s) in Sentry codebase


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


def get_key(event):
    # send the same (project_id, event_id) to the same kafka partition
    project_id = event['project_id']
    event_id = event['event_id']
    return '%s:%s' % (project_id, event_id)


def process_raw_event(event):
    processed = {}

    processed['event_id'] = event['event_id']

    # TODO: remove splice and rjust once we handle 'checksum' hashes (which are too long)
    processed['primary_hash'] = event['primary_hash'][-16:].rjust(16)

    processed['project_id'] = event['project_id']
    processed['message'] = _unicodify(event['message'])
    processed['platform'] = _unicodify(event['platform'])
    processed['timestamp'] = int(time.mktime(
        datetime.strptime(
            event['datetime'],
            "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))

    data = event.get('data', {})

    processed['received'] = int(data['received'])

    sdk = data.get('sdk', {})
    processed['sdk_name'] = _unicodify(sdk.get('name', None))
    processed['sdk_version'] = _unicodify(sdk.get('version', None))

    tags = dict(data.get('tags', []))

    tags.pop('sentry:user', None)  # defer to user interface data (below)
    processed['level'] = _unicodify(tags.pop('level', None))
    processed['logger'] = _unicodify(tags.pop('logger', None))
    processed['server_name'] = _unicodify(tags.pop('server_name', None))
    processed['transaction'] = _unicodify(tags.pop('transaction', None))
    processed['environment'] = _unicodify(tags.pop('environment', None))
    processed['release'] = _unicodify(tags.pop('sentry:release', None))
    processed['dist'] = _unicodify(tags.pop('sentry:dist', None))
    processed['site'] = _unicodify(tags.pop('site', None))
    processed['url'] = _unicodify(tags.pop('url', None))

    user = data.get('sentry.interfaces.User', {})
    processed['user_id'] = _unicodify(user.get('id', None))
    processed['username'] = _unicodify(user.get('username', None))
    processed['email'] = _unicodify(user.get('email', None))
    processed['ip_address'] = _unicodify(user.get('ip_address', None))

    http = data.get('sentry.interfaces.Http', {})
    processed['http_method'] = _unicodify(http.get('method', None))

    http_headers = dict(http.get('headers', []))
    processed['http_referer'] = _unicodify(http_headers.get('Referer', None))

    tag_keys = []
    tag_values = []
    for tag_key, tag_value in tags.items():
        tag_keys.append(_unicodify(tag_key))
        tag_values.append(_unicodify(tag_value))

    processed['tags.key'] = tag_keys
    processed['tags.value'] = tag_values

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

    processed['exception_stacks.type'] = stack_types
    processed['exception_stacks.value'] = stack_values
    processed['exception_frames.abs_path'] = frame_abs_paths
    processed['exception_frames.filename'] = frame_filenames
    processed['exception_frames.package'] = frame_packages
    processed['exception_frames.module'] = frame_modules
    processed['exception_frames.function'] = frame_functions
    processed['exception_frames.in_app'] = frame_in_app
    processed['exception_frames.colno'] = frame_colnos
    processed['exception_frames.lineno'] = frame_linenos
    processed['exception_frames.stack_level'] = frame_stack_levels

    return processed
