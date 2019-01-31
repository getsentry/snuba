from datetime import datetime, timedelta
from hashlib import md5
import logging
import re
import simplejson as json
import six
import _strptime  # fixes _strptime deferred import issue

from snuba import settings
from snuba.clickhouse import PROMOTED_TAG_COLUMNS
from snuba.util import force_bytes


logger = logging.getLogger('snuba.processor')


HASH_RE = re.compile(r'^[0-9a-f]{32}$', re.IGNORECASE)
MAX_UINT32 = 2 ** 32 - 1

# action types
INSERT = object()
REPLACE = object()


class EventTooOld(Exception):
    pass


def _as_dict_safe(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    rv = {}
    for item in value:
        if item is not None:
            rv[item[0]] = item[1]
    return rv


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


def _hashify(h):
    if HASH_RE.match(h):
        return h
    return md5(force_bytes(h)).hexdigest()


def extract_required(output, message):
    output['event_id'] = message['event_id']
    project_id = message['project_id']
    output['project_id'] = project_id
    output['group_id'] = message['group_id']
    timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)

    retention_days = settings.RETENTION_OVERRIDES.get(project_id)
    if retention_days is None:
        retention_days = int(message.get('retention_days') or settings.DEFAULT_RETENTION_DAYS)

    # TODO: We may need to allow for older events in the future when post
    # processing triggers are based off of Snuba. Or this branch could be put
    # behind a "backfill-only" optional switch.
    if settings.DISCARD_OLD_EVENTS and timestamp < (datetime.utcnow() - timedelta(days=retention_days)):
        raise EventTooOld

    output['timestamp'] = timestamp
    output['retention_days'] = retention_days


def extract_common(output, message, data):
    # Properties we get from the top level of the message payload
    output['platform'] = _unicodify(message['platform'])
    output['primary_hash'] = _hashify(message['primary_hash'])

    # Properties we get from the "data" dict, which is the actual event body.
    output['received'] = datetime.utcfromtimestamp(int(data['received']))
    output['culprit'] = _unicodify(data.get('culprit', None))
    output['type'] = _unicodify(data.get('type', None))
    output['version'] = _unicodify(data.get('version', None))
    output['title'] = _unicodify(data.get('title', None))
    output['location'] = _unicodify(data.get('location', None))

    # The following concerns the change to message/search_message
    # There are 2 Scenarios:
    #   Pre-rename:
    #        - Payload contains:
    #             "message": "a long search message"
    #        - Does NOT contain a `search_message` property
    #        - "message" value saved in `message` column
    #        - `search_message` column nonexistent or Null
    #   Post-rename:
    #        - Payload contains:
    #             "search_message": "a long search message"
    #        - Optionally the payload's "data" dict (event body) contains:
    #             "message": "short message"
    #        - "search_message" value stored in `search_message` column
    #        - "message" value stored in `message` column
    #
    # When querying the data / searching:
    #   If search_message is Null, the event is from pre-rename so the search message
    #   is stored in the `message` column.
    #   Otherwise search in the `search_message` column.
    #   Practically we can achieve this by searching `coalesce(search_message, message)`
    output['search_message'] = _unicodify(message.get('search_message', None))
    if output['search_message'] is None:
        # Pre-rename scenario, we expect to find "message" at the top level
        output['message'] = _unicodify(message['message'])
    else:
        # Post-rename scenario, we check in case we have the optional
        # "message" in the event body.
        output['message'] = _unicodify(data.get('message', None))

    module_names = []
    module_versions = []
    modules = data.get('modules', {})
    if isinstance(modules, dict):
        for name, version in modules.items():
            module_names.append(_unicodify(name))
            # Being extra careful about a stray (incorrect by spec) `null`
            # value blowing up the write.
            module_versions.append(_unicodify(version) or '')

    output['modules.name'] = module_names
    output['modules.version'] = module_versions


def extract_sdk(output, sdk):
    output['sdk_name'] = _unicodify(sdk.get('name', None))
    output['sdk_version'] = _unicodify(sdk.get('version', None))

    sdk_integrations = []
    for i in sdk.get('integrations', None) or ():
        i = _unicodify(i)
        if i:
            sdk_integrations.append(i)
    output['sdk_integrations'] = sdk_integrations


def extract_promoted_tags(output, tags):
    output.update({col.name: _unicodify(tags.get(col.name, None))
        for col in PROMOTED_TAG_COLUMNS
    })


def extract_promoted_contexts(output, contexts, tags):
    app_ctx = contexts.get('app', None) or {}
    output['app_device'] = _unicodify(tags.get('app.device', None))
    app_ctx.pop('device_app_hash', None)  # tag=app.device

    os_ctx = contexts.get('os', None) or {}
    output['os'] = _unicodify(tags.get('os', None))
    output['os_name'] = _unicodify(tags.get('os.name', None))
    os_ctx.pop('name', None)  # tag=os and/or os.name
    os_ctx.pop('version', None)  # tag=os
    output['os_rooted'] = _boolify(tags.get('os.rooted', None))
    os_ctx.pop('rooted', None)  # tag=os.rooted
    output['os_build'] = _unicodify(os_ctx.pop('build', None))
    output['os_kernel_version'] = _unicodify(os_ctx.pop('kernel_version', None))

    runtime_ctx = contexts.get('runtime', None) or {}
    output['runtime'] = _unicodify(tags.get('runtime', None))
    output['runtime_name'] = _unicodify(tags.get('runtime.name', None))
    runtime_ctx.pop('name', None)  # tag=runtime and/or runtime.name
    runtime_ctx.pop('version', None)  # tag=runtime

    browser_ctx = contexts.get('browser', None) or {}
    output['browser'] = _unicodify(tags.get('browser', None))
    output['browser_name'] = _unicodify(tags.get('browser.name', None))
    browser_ctx.pop('name', None)  # tag=browser and/or browser.name
    browser_ctx.pop('version', None)  # tag=browser

    device_ctx = contexts.get('device', None) or {}
    output['device'] = _unicodify(tags.get('device', None))
    device_ctx.pop('model', None)  # tag=device
    output['device_family'] = _unicodify(tags.get('device.family', None))
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
    for tag_key, tag_value in sorted(tags.items()):
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


def extract_geo(output, geo):
    output['geo_country_code'] = _unicodify(geo.get('country_code', None))
    output['geo_region'] = _unicodify(geo.get('region', None))
    output['geo_city'] = _unicodify(geo.get('city', None))


def extract_http(output, http):
    output['http_method'] = _unicodify(http.get('method', None))
    http_headers = _as_dict_safe(http.get('headers', None))
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
        if stack is None:
            continue

        stack_types.append(_unicodify(stack.get('type', None)))
        stack_values.append(_unicodify(stack.get('value', None)))

        mechanism = stack.get('mechanism', None) or {}
        stack_mechanism_types.append(_unicodify(mechanism.get('type', None)))
        stack_mechanism_handled.append(_boolify(mechanism.get('handled', None)))

        frames = (stack.get('stacktrace', None) or {}).get('frames', None) or []
        for frame in frames:
            if frame is None:
                continue

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


class InvalidMessageType(Exception):
    pass


class InvalidMessageVersion(Exception):
    pass


def process_message(message):
    """\
    Process a raw message into a tuple of (action_type, processed_message):
    * action_type: one of the sentinel values INSERT or REPLACE
    * processed_message: dict representing the processed column -> value(s)

    Returns `None` if the event is too old to be written.
    """
    action_type = None

    if isinstance(message, dict):
        # deprecated unwrapped event message == insert
        action_type = INSERT
        try:
            processed = process_insert(message)
        except EventTooOld:
            return None
    elif isinstance(message, (list, tuple)) and len(message) >= 2:
        version = message[0]

        if version in (0, 1, 2):
            # version 0: (0, 'insert', data)
            # version 1: (1, type, data, [state])
            #   NOTE: types 'delete_groups', 'merge' and 'unmerge' are ignored
            # version 2: (2, type, data, [state])
            type_, event = message[1:3]
            if type_ == 'insert':
                action_type = INSERT

                if str(event.get("project", None)) == "1041156":
                    return None

                try:
                    processed = process_insert(event)
                except EventTooOld:
                    return None
            else:
                if version == 0:
                    raise InvalidMessageType("Invalid message type: {}".format(type_))
                elif version == 1:
                    if type_ in ('delete_groups', 'merge', 'unmerge'):
                        # these didn't contain the necessary data to handle replacements
                        return None
                    else:
                        raise InvalidMessageType("Invalid message type: {}".format(type_))
                elif version == 2:
                    # we temporarily sent these invalid message types from Sentry
                    if type_ in ('delete_groups', 'merge'):
                        return None

                    if type_ in ('start_delete_groups', 'start_merge', 'start_unmerge',
                                 'end_delete_groups', 'end_merge', 'end_unmerge'):
                        # pass raw events along to republish
                        action_type = REPLACE
                        processed = (six.text_type(event['project_id']), message)
                    else:
                        raise InvalidMessageType("Invalid message type: {}".format(type_))

    if action_type is None:
        raise InvalidMessageVersion("Unknown message format: " + str(message))

    if processed is None:
        return None

    return (action_type, processed)


def process_insert(message):
    processed = {'deleted': 0}
    extract_required(processed, message)

    data = message.get('data', {})
    # HACK: https://sentry.io/sentry/snuba/issues/802102397/
    if not data:
        logger.error('No data for event: %s', message, exc_info=True)
        return None
    extract_common(processed, message, data)

    sdk = data.get('sdk', None) or {}
    extract_sdk(processed, sdk)

    tags = _as_dict_safe(data.get('tags', None))
    extract_promoted_tags(processed, tags)

    contexts = data.get('contexts', None) or {}
    extract_promoted_contexts(processed, contexts, tags)

    user = data.get('user', data.get('sentry.interfaces.User', None)) or {}
    extract_user(processed, user)

    geo = user.get('geo', None) or {}
    extract_geo(processed, geo)

    http = data.get('request', data.get('sentry.interfaces.Http', None)) or {}
    extract_http(processed, http)

    extract_extra_contexts(processed, contexts)
    extract_extra_tags(processed, tags)

    exception = data.get('exception', data.get('sentry.interfaces.Exception', None)) or {}
    stacks = exception.get('values', None) or []
    extract_stacktraces(processed, stacks)

    return processed
