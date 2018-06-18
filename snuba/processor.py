import calendar
import logging
import simplejson as json
import six
import re
from datetime import datetime
from hashlib import md5

from snuba import settings
from snuba.consumer import AbstractBatchWorker
from snuba.util import force_bytes


logger = logging.getLogger('snuba.processor')


HASH_RE = re.compile(r'^[0-9a-f]{32}$', re.IGNORECASE)
MAX_UINT32 = 2 * 32 - 1
INSERT = object()
DELETE = object()


def _collapse_uint32(n):
    if (n is None) or (n < 0) or (n > MAX_UINT32):
        return None
    return n


def _boolify(s):
    if s is None:
        return None

    if isinstance(s, bool):
        return s

    s = _unicodify(s)

    if s in ('yes', 'true', '1'):
        return True
    elif s in ('false', 'no', '0'):
        return False

    return None


def _floatify(s):
    if not s:
        return None

    if isinstance(s, float):
        return s

    try:
        s = float(s)
    except (ValueError, TypeError):
        return None
    else:
        return s

    return None


def _unicodify(s):
    if s is None:
        return None

    if isinstance(s, dict) or isinstance(s, list):
        return json.dumps(s)

    return six.text_type(s)


def get_key(message):
    # send the same (project_id, event_id) to the same kafka partition
    project_id = message['project_id']
    event_id = message['event_id']
    return '%s:%s' % (project_id, event_id)


def extract_required(output, message):
    output['event_id'] = message['event_id']
    project_id = message['project_id']
    output['project_id'] = project_id
    output['timestamp'] = int(calendar.timegm(
        datetime.strptime(
            message['datetime'],
            "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))

    retention_days = settings.RETENTION_OVERRIDES.get(project_id)
    if retention_days is None:
        retention_days = int(message.get('retention_days') or settings.DEFAULT_RETENTION_DAYS)

    output['retention_days'] = retention_days


def extract_common(output, message, data):
    output['platform'] = _unicodify(message['platform'])
    output['message'] = _unicodify(message['message'])
    primary_hash = message['primary_hash']
    if not HASH_RE.match(primary_hash):
        primary_hash = md5(force_bytes(primary_hash)).hexdigest()
    output['primary_hash'] = primary_hash
    output['received'] = int(data['received'])


def extract_sdk(output, sdk):
    output['sdk_name'] = _unicodify(sdk.get('name', None))
    output['sdk_version'] = _unicodify(sdk.get('version', None))


def extract_promoted_tags(output, tags):
    output.update({name: _unicodify(tags.pop(name, None))
        for name in settings.PROMOTED_TAGS
    })


def extract_promoted_contexts(output, contexts, tags):
    app_ctx = contexts.get('app', {})
    output['app_device'] = _unicodify(tags.pop('app.device', None))
    app_ctx.pop('device_app_hash', None)  # tag=app.device

    os_ctx = contexts.get('os', {})
    output['os'] = _unicodify(tags.pop('os', None))
    output['os_name'] = _unicodify(tags.pop('os.name', None))
    os_ctx.pop('name', None)  # tag=os and/or os.name
    os_ctx.pop('version', None)  # tag=os
    output['os_rooted'] = _boolify(tags.pop('os.rooted', None))
    os_ctx.pop('rooted', None)  # tag=os.rooted
    output['os_build'] = _unicodify(os_ctx.pop('build', None))
    output['os_kernel_version'] = _unicodify(os_ctx.pop('kernel_version', None))

    runtime_ctx = contexts.get('runtime', {})
    output['runtime'] = _unicodify(tags.pop('runtime', None))
    output['runtime_name'] = _unicodify(tags.pop('runtime.name', None))
    runtime_ctx.pop('name', None)  # tag=runtime and/or runtime.name
    runtime_ctx.pop('version', None)  # tag=runtime

    browser_ctx = contexts.get('browser', {})
    output['browser'] = _unicodify(tags.pop('browser', None))
    output['browser_name'] = _unicodify(tags.pop('browser.name', None))
    browser_ctx.pop('name', None)  # tag=browser and/or browser.name
    browser_ctx.pop('version', None)  # tag=browser

    device_ctx = contexts.get('device', {})
    output['device'] = _unicodify(tags.pop('device', None))
    device_ctx.pop('model', None)  # tag=device
    output['device_family'] = _unicodify(tags.pop('device.family', None))
    device_ctx.pop('family', None)  # tag=device.family
    output['device_name'] = _unicodify(device_ctx.pop('name', None))
    output['device_brand'] = _unicodify(device_ctx.pop('brand', None))
    output['device_locale'] = _unicodify(device_ctx.pop('locale', None))
    output['device_uuid'] = _unicodify(device_ctx.pop('uuid', None))
    output['device_model_id'] = _unicodify(device_ctx.pop('model_id', None))
    output['device_arch'] = _unicodify(device_ctx.pop('arch', None))
    output['device_battery_level'] = _floatify(device_ctx.pop('battery_level', None))
    output['device_orientation'] = _unicodify(device_ctx.pop('orientation', None))
    output['device_simulator'] = _boolify(device_ctx.pop('simulator', None))
    output['device_online'] = _boolify(device_ctx.pop('online', None))
    output['device_charging'] = _boolify(device_ctx.pop('charging', None))


def extract_extra_contexts(output, contexts):
    context_keys = []
    context_values = []
    valid_types = (int, float) + six.string_types
    for ctx_name, ctx_obj in contexts.items():
        if isinstance(ctx_obj, dict):
            ctx_obj.pop('type', None)  # ignore type alias
            for inner_ctx_name, ctx_value in ctx_obj.items():
                if isinstance(ctx_value, valid_types):
                    value = _unicodify(ctx_value)
                    if value:
                        context_keys.append("%s.%s" % (ctx_name, inner_ctx_name))
                        context_values.append(_unicodify(ctx_value))

    output['contexts.key'] = context_keys
    output['contexts.value'] = context_values


def extract_extra_tags(output, tags):
    tag_keys = []
    tag_values = []
    for tag_key, tag_value in tags.items():
        value = _unicodify(tag_value)
        if value:
            tag_keys.append(_unicodify(tag_key))
            tag_values.append(value)

    output['tags.key'] = tag_keys
    output['tags.value'] = tag_values


def extract_user(output, user):
    output['user_id'] = _unicodify(user.get('id', None))
    output['username'] = _unicodify(user.get('username', None))
    output['email'] = _unicodify(user.get('email', None))
    output['ip_address'] = _unicodify(user.get('ip_address', None))


def extract_http(output, http):
    output['http_method'] = _unicodify(http.get('method', None))
    http_headers = dict(http.get('headers', []))
    output['http_referer'] = _unicodify(http_headers.get('Referer', None))


def extract_stacktraces(output, stacks):
    stack_types = []
    stack_values = []
    stack_mechanism_types = []
    stack_mechanism_handled = []

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
    for stack in stacks:
        stack_types.append(_unicodify(stack.get('type', None)))
        stack_values.append(_unicodify(stack.get('value', None)))

        mechanism = stack.get('mechanism', {})
        stack_mechanism_types.append(_unicodify(mechanism.get('type', None)))
        stack_mechanism_handled.append(_boolify(mechanism.get('handled', None)))

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

    output['exception_stacks.type'] = stack_types
    output['exception_stacks.value'] = stack_values
    output['exception_stacks.mechanism_type'] = stack_mechanism_types
    output['exception_stacks.mechanism_handled'] = stack_mechanism_handled
    output['exception_frames.abs_path'] = frame_abs_paths
    output['exception_frames.filename'] = frame_filenames
    output['exception_frames.package'] = frame_packages
    output['exception_frames.module'] = frame_modules
    output['exception_frames.function'] = frame_functions
    output['exception_frames.in_app'] = frame_in_app
    output['exception_frames.colno'] = frame_colnos
    output['exception_frames.lineno'] = frame_linenos
    output['exception_frames.stack_level'] = frame_stack_levels


def process_message(message):
    """\
    Process a raw message into a tuple of (message_type, key, processed_message):
    * message_type: one of the sentinel values INSERT or DELETE
    * key: string used for output partitioning
    * processed_message: dict representing the processed column -> value(s)
    """
    message_type = None

    if isinstance(message, dict):
        # deprecated unwrapped event message == insert
        message_type = INSERT
        processed = process_insert(message)
    elif isinstance(message, (list, tuple)) and len(message) >= 2:
        version = message[0]

        if version == 0:
            # version 0: (version, type, message)
            type_, event = message[1:]
            if type_ == 'insert':
                message_type = INSERT
                processed = process_insert(event)
            elif type_ == 'delete':
                message_type = DELETE
                processed = process_delete(event)

    if message_type is None:
        raise ValueError("Unknown message format: " + str(message))

    key = get_key(processed).encode('utf-8')
    return (message_type, key, processed)


def process_insert(message):
    processed = {'deleted': 0}
    extract_required(processed, message)

    data = message.get('data', {})
    extract_common(processed, message, data)

    sdk = data.get('sdk', {})
    extract_sdk(processed, sdk)

    tags = dict(data.get('tags', []))
    extract_promoted_tags(processed, tags)

    contexts = data.get('contexts', {})
    extract_promoted_contexts(processed, contexts, tags)

    user = data.get('sentry.interfaces.User', {})
    extract_user(processed, user)

    http = data.get('sentry.interfaces.Http', {})
    extract_http(processed, http)

    extract_extra_contexts(processed, contexts)
    extract_extra_tags(processed, tags)

    stacks = data.get('sentry.interfaces.Exception', {}).get('values', [])
    extract_stacktraces(processed, stacks)

    return processed


def process_delete(message):
    processed = {'deleted': 1}
    extract_required(processed, message)

    return processed


class ProcessorWorker(AbstractBatchWorker):
    def __init__(self, producer, events_topic, deletes_topic):
        self.producer = producer
        self.events_topic = events_topic
        self.deletes_topics = deletes_topic

    def process_message(self, message):
        value = json.loads(message.value())

        message_type, key, processed = process_message(value)
        processed['offset'] = message.offset()
        processed['partition'] = message.partition()

        return (message_type, key, json.dumps(processed).encode('utf-8'))

    def flush_batch(self, batch):
        for message_type, key, value in batch:
            if message_type is INSERT:
                topic = self.events_topic
            elif message_type is DELETE:
                topic = self.deletes_topics

            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
            )

        self.producer.flush()

    def shutdown(self):
        self.producer.flush()
