from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple

import logging
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba import settings
from snuba.processor import (
    _as_dict_safe,
    _boolify,
    _collapse_uint32,
    _ensure_valid_date,
    _ensure_valid_ip,
    _floatify,
    _hashify,
    _unicodify,
    InvalidMessageType,
    InvalidMessageVersion,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
)

logger = logging.getLogger("snuba.processor")


def extract_base(output, message):
    output["event_id"] = message["event_id"]
    project_id = message["project_id"]
    output["project_id"] = project_id
    return output


def extract_user(output, user):
    output["user_id"] = _unicodify(user.get("id", None))
    output["username"] = _unicodify(user.get("username", None))
    output["email"] = _unicodify(user.get("email", None))
    ip_addr = _ensure_valid_ip(user.get("ip_address", None))
    output["ip_address"] = str(ip_addr) if ip_addr is not None else None


def extract_extra_tags(tags) -> Tuple[Sequence[str], Sequence[str]]:
    tag_keys = []
    tag_values = []
    for tag_key, tag_value in sorted(tags.items()):
        value = _unicodify(tag_value)
        if value:
            tag_keys.append(_unicodify(tag_key))
            tag_values.append(value)

    return (tag_keys, tag_values)


def extract_extra_contexts(contexts) -> Tuple[Sequence[str], Sequence[str]]:
    context_keys = []
    context_values = []
    valid_types = (int, float, str)
    for ctx_name, ctx_obj in contexts.items():
        if isinstance(ctx_obj, dict):
            ctx_obj.pop("type", None)  # ignore type alias
            for inner_ctx_name, ctx_value in ctx_obj.items():
                if isinstance(ctx_value, valid_types):
                    value = _unicodify(ctx_value)
                    if value:
                        ctx_key = f"{ctx_name}.{inner_ctx_name}"
                        context_keys.append(_unicodify(ctx_key))
                        context_values.append(_unicodify(ctx_value))

    return (context_keys, context_values)


def enforce_retention(message, timestamp):
    project_id = message["project_id"]
    retention_days = settings.RETENTION_OVERRIDES.get(project_id)
    if retention_days is None:
        retention_days = int(
            message.get("retention_days") or settings.DEFAULT_RETENTION_DAYS
        )

    # This is not ideal but it should never happen anyways
    timestamp = _ensure_valid_date(timestamp)
    if timestamp is None:
        timestamp = datetime.utcnow()
    # TODO: We may need to allow for older events in the future when post
    # processing triggers are based off of Snuba. Or this branch could be put
    # behind a "backfill-only" optional switch.
    if settings.DISCARD_OLD_EVENTS and timestamp < (
        datetime.utcnow() - timedelta(days=retention_days)
    ):
        raise EventTooOld
    return retention_days


class EventsProcessor(MessageProcessor):
    def __init__(self, promoted_tag_columns):
        self.__promoted_tag_columns = promoted_tag_columns

    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        """\
        Process a raw message into a tuple of (action_type, processed_message):
        * action_type: one of the sentinel values INSERT or REPLACE
        * processed_message: dict representing the processed column -> value(s)

        Returns `None` if the event is too old to be written.
        """
        action_type = None

        if isinstance(message, dict):
            # deprecated unwrapped event message == insert
            action_type = ProcessorAction.INSERT
            try:
                processed = self.process_insert(message, metadata)
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
                if type_ == "insert":
                    action_type = ProcessorAction.INSERT
                    try:
                        processed = self.process_insert(event, metadata)
                    except EventTooOld:
                        return None
                else:
                    if version == 0:
                        raise InvalidMessageType(
                            "Invalid message type: {}".format(type_)
                        )
                    elif version == 1:
                        if type_ in ("delete_groups", "merge", "unmerge"):
                            # these didn't contain the necessary data to handle replacements
                            return None
                        else:
                            raise InvalidMessageType(
                                "Invalid message type: {}".format(type_)
                            )
                    elif version == 2:
                        # we temporarily sent these invalid message types from Sentry
                        if type_ in ("delete_groups", "merge"):
                            return None

                        if type_ in (
                            "start_delete_groups",
                            "start_merge",
                            "start_unmerge",
                            "start_delete_tag",
                            "end_delete_groups",
                            "end_merge",
                            "end_unmerge",
                            "end_delete_tag",
                        ):
                            # pass raw events along to republish
                            action_type = ProcessorAction.REPLACE
                            processed = (str(event["project_id"]), message)
                        else:
                            raise InvalidMessageType(
                                "Invalid message type: {}".format(type_)
                            )

        if action_type is None:
            raise InvalidMessageVersion("Unknown message format: " + str(message))

        if processed is None:
            return None

        return ProcessedMessage(action=action_type, data=[processed],)

    def process_insert(self, message, metadata=None):
        processed = {"deleted": 0}
        extract_base(processed, message)
        processed["retention_days"] = enforce_retention(
            message,
            datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT),
        )

        self.extract_required(processed, message)

        data = message.get("data", {})
        # HACK: https://sentry.io/sentry/snuba/issues/802102397/
        if not data:
            logger.error("No data for event: %s", message, exc_info=True)
            return None
        self.extract_common(processed, message, data)

        sdk = data.get("sdk", None) or {}
        self.extract_sdk(processed, sdk)

        tags = _as_dict_safe(data.get("tags", None))
        self.extract_promoted_tags(processed, tags)

        contexts = data.get("contexts", None) or {}
        self.extract_promoted_contexts(processed, contexts, tags)

        user = data.get("user", data.get("sentry.interfaces.User", None)) or {}
        extract_user(processed, user)

        geo = user.get("geo", None) or {}
        self.extract_geo(processed, geo)

        http = data.get("request", data.get("sentry.interfaces.Http", None)) or {}
        self.extract_http(processed, http)

        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            contexts
        )
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        exception = (
            data.get("exception", data.get("sentry.interfaces.Exception", None)) or {}
        )
        stacks = exception.get("values", None) or []
        self.extract_stacktraces(processed, stacks)

        if metadata is not None:
            processed["offset"] = metadata.offset
            processed["partition"] = metadata.partition

        return processed

    def extract_required(self, output, message):
        output["group_id"] = message["group_id"] or 0

        # This is not ideal but it should never happen anyways
        timestamp = _ensure_valid_date(
            datetime.strptime(message["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
        )
        if timestamp is None:
            timestamp = datetime.utcnow()

        output["timestamp"] = timestamp

    def extract_common(self, output, message, data):
        # Properties we get from the top level of the message payload
        output["platform"] = _unicodify(message["platform"])
        output["primary_hash"] = _hashify(message["primary_hash"])

        # Properties we get from the "data" dict, which is the actual event body.
        received = _collapse_uint32(int(data["received"]))
        output["received"] = (
            datetime.utcfromtimestamp(received) if received is not None else None
        )

        output["culprit"] = _unicodify(data.get("culprit", None))
        output["type"] = _unicodify(data.get("type", None))
        output["version"] = _unicodify(data.get("version", None))
        output["title"] = _unicodify(data.get("title", None))
        output["location"] = _unicodify(data.get("location", None))

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
        output["search_message"] = _unicodify(message.get("search_message", None))
        if output["search_message"] is None:
            # Pre-rename scenario, we expect to find "message" at the top level
            output["message"] = _unicodify(message["message"])
        else:
            # Post-rename scenario, we check in case we have the optional
            # "message" in the event body.
            output["message"] = _unicodify(data.get("message", None))

        module_names = []
        module_versions = []
        modules = data.get("modules", {})
        if isinstance(modules, dict):
            for name, version in modules.items():
                module_names.append(_unicodify(name))
                # Being extra careful about a stray (incorrect by spec) `null`
                # value blowing up the write.
                module_versions.append(_unicodify(version) or "")

        output["modules.name"] = module_names
        output["modules.version"] = module_versions

    def extract_sdk(self, output, sdk):
        output["sdk_name"] = _unicodify(sdk.get("name", None))
        output["sdk_version"] = _unicodify(sdk.get("version", None))

        sdk_integrations = []
        for i in sdk.get("integrations", None) or ():
            i = _unicodify(i)
            if i:
                sdk_integrations.append(i)
        output["sdk_integrations"] = sdk_integrations

    def extract_promoted_tags(self, output, tags):
        output.update(
            {
                col.name: _unicodify(tags.get(col.name, None))
                for col in self.__promoted_tag_columns
            }
        )

    def extract_promoted_contexts(self, output, contexts, tags):
        app_ctx = contexts.get("app", None) or {}
        output["app_device"] = _unicodify(tags.get("app.device", None))
        app_ctx.pop("device_app_hash", None)  # tag=app.device

        os_ctx = contexts.get("os", None) or {}
        output["os"] = _unicodify(tags.get("os", None))
        output["os_name"] = _unicodify(tags.get("os.name", None))
        os_ctx.pop("name", None)  # tag=os and/or os.name
        os_ctx.pop("version", None)  # tag=os
        output["os_rooted"] = _boolify(tags.get("os.rooted", None))
        os_ctx.pop("rooted", None)  # tag=os.rooted
        output["os_build"] = _unicodify(os_ctx.pop("build", None))
        output["os_kernel_version"] = _unicodify(os_ctx.pop("kernel_version", None))

        runtime_ctx = contexts.get("runtime", None) or {}
        output["runtime"] = _unicodify(tags.get("runtime", None))
        output["runtime_name"] = _unicodify(tags.get("runtime.name", None))
        runtime_ctx.pop("name", None)  # tag=runtime and/or runtime.name
        runtime_ctx.pop("version", None)  # tag=runtime

        browser_ctx = contexts.get("browser", None) or {}
        output["browser"] = _unicodify(tags.get("browser", None))
        output["browser_name"] = _unicodify(tags.get("browser.name", None))
        browser_ctx.pop("name", None)  # tag=browser and/or browser.name
        browser_ctx.pop("version", None)  # tag=browser

        device_ctx = contexts.get("device", None) or {}
        output["device"] = _unicodify(tags.get("device", None))
        device_ctx.pop("model", None)  # tag=device
        output["device_family"] = _unicodify(tags.get("device.family", None))
        device_ctx.pop("family", None)  # tag=device.family
        output["device_name"] = _unicodify(device_ctx.pop("name", None))
        output["device_brand"] = _unicodify(device_ctx.pop("brand", None))
        output["device_locale"] = _unicodify(device_ctx.pop("locale", None))
        output["device_uuid"] = _unicodify(device_ctx.pop("uuid", None))
        output["device_model_id"] = _unicodify(device_ctx.pop("model_id", None))
        output["device_arch"] = _unicodify(device_ctx.pop("arch", None))
        output["device_battery_level"] = _floatify(
            device_ctx.pop("battery_level", None)
        )
        output["device_orientation"] = _unicodify(device_ctx.pop("orientation", None))
        output["device_simulator"] = _boolify(device_ctx.pop("simulator", None))
        output["device_online"] = _boolify(device_ctx.pop("online", None))
        output["device_charging"] = _boolify(device_ctx.pop("charging", None))

    def extract_geo(self, output, geo):
        output["geo_country_code"] = _unicodify(geo.get("country_code", None))
        output["geo_region"] = _unicodify(geo.get("region", None))
        output["geo_city"] = _unicodify(geo.get("city", None))

    def extract_http(self, output, http):
        output["http_method"] = _unicodify(http.get("method", None))
        http_headers = _as_dict_safe(http.get("headers", None))
        output["http_referer"] = _unicodify(http_headers.get("Referer", None))

    def extract_stacktraces(self, output, stacks):
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

        if output["project_id"] not in settings.PROJECT_STACKTRACE_BLACKLIST:
            stack_level = 0
            for stack in stacks:
                if stack is None:
                    continue

                stack_types.append(_unicodify(stack.get("type", None)))
                stack_values.append(_unicodify(stack.get("value", None)))

                mechanism = stack.get("mechanism", None) or {}
                stack_mechanism_types.append(_unicodify(mechanism.get("type", None)))
                stack_mechanism_handled.append(_boolify(mechanism.get("handled", None)))

                frames = (stack.get("stacktrace", None) or {}).get("frames", None) or []
                for frame in frames:
                    if frame is None:
                        continue

                    frame_abs_paths.append(_unicodify(frame.get("abs_path", None)))
                    frame_filenames.append(_unicodify(frame.get("filename", None)))
                    frame_packages.append(_unicodify(frame.get("package", None)))
                    frame_modules.append(_unicodify(frame.get("module", None)))
                    frame_functions.append(_unicodify(frame.get("function", None)))
                    frame_in_app.append(frame.get("in_app", None))
                    frame_colnos.append(_collapse_uint32(frame.get("colno", None)))
                    frame_linenos.append(_collapse_uint32(frame.get("lineno", None)))
                    frame_stack_levels.append(stack_level)

                stack_level += 1

        output["exception_stacks.type"] = stack_types
        output["exception_stacks.value"] = stack_values
        output["exception_stacks.mechanism_type"] = stack_mechanism_types
        output["exception_stacks.mechanism_handled"] = stack_mechanism_handled
        output["exception_frames.abs_path"] = frame_abs_paths
        output["exception_frames.filename"] = frame_filenames
        output["exception_frames.package"] = frame_packages
        output["exception_frames.module"] = frame_modules
        output["exception_frames.function"] = frame_functions
        output["exception_frames.in_app"] = frame_in_app
        output["exception_frames.colno"] = frame_colnos
        output["exception_frames.lineno"] = frame_linenos
        output["exception_frames.stack_level"] = frame_stack_levels


class EventTooOld(Exception):
    pass
