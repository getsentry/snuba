import logging
import uuid
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Sequence, cast

import _strptime  # NOQA fixes _strptime deferred import issue
from sentry_kafka_schemas.schema_types.events_v1 import (
    ClientSdkInfo,
    EventStreamMessage,
    InsertEvent,
    SentryExceptionChain,
    SentryRequest,
    SentryThreadChain,
    SentryUser,
)

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_contexts,
    extract_extra_tags,
    extract_http,
    extract_project_id,
    extract_user,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    REPLACEMENT_EVENT_TYPES,
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ProcessedMessage,
    ReplacementBatch,
    _as_dict_safe,
    _boolify,
    _collapse_uint32,
    _ensure_valid_date,
    _ensure_valid_ip,
    _hashify,
    _unicodify,
)

logger = logging.getLogger(__name__)


class ErrorsProcessor(DatasetMessageProcessor):
    def __init__(self) -> None:
        self._promoted_tag_columns = {
            "environment": "environment",
            "sentry:release": "release",
            "sentry:dist": "dist",
            "sentry:user": "user",
            "transaction": "transaction_name",
            "level": "level",
        }

    def process_message(
        self,
        message: EventStreamMessage,
        metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        """\
        Process a raw message into an insertion or replacement batch. Returns
        `None` if the event is too old to be written.
        """
        version = message[0]
        if version != 2:
            raise InvalidMessageVersion(f"Unsupported message version: {version}")

        # version 2: (2, type, data, [state])
        type_, event = message[1:3]
        if type_ == "insert":
            try:
                row = self.process_insert(cast(InsertEvent, event), metadata)
            except EventTooOld:
                return None

            if row is None:  # the processor cannot/does not handle this input
                return None

            return InsertBatch([row], row["received"])
        elif type_ in REPLACEMENT_EVENT_TYPES:
            # pass raw events along to republish
            return ReplacementBatch(str(event["project_id"]), [message])
        else:
            raise InvalidMessageType(f"Invalid message type: {type_}")

    def process_insert(
        self, event: InsertEvent, metadata: KafkaMessageMetadata
    ) -> Optional[Mapping[str, Any]]:
        if not self._should_process(event):
            return None

        processed: MutableMapping[str, Any] = {"deleted": 0}
        extract_project_id(processed, event)
        self._extract_event_id(processed, event)
        processed["retention_days"] = enforce_retention(
            event.get("retention_days"),
            datetime.strptime(event["datetime"], settings.PAYLOAD_DATETIME_FORMAT),
        )

        self.extract_required(processed, event)

        data = event.get("data", {})
        # HACK: https://sentry.io/sentry/snuba/issues/802102397/
        if not data:
            logger.error("No data for event: %s", event, exc_info=True)
            return None
        self.extract_common(processed, event, metadata)
        contexts = self.extract_custom(processed, event, metadata)

        sdk: ClientSdkInfo = data.get("sdk", None) or {
            "name": "",
            "version": "",
        }
        self.extract_sdk(processed, sdk)

        # XXX(markus): tags should actually only be of the array form
        tags = _as_dict_safe(data.get("tags", None))
        self.extract_promoted_tags(processed, tags)
        self.extract_tags_custom(processed, event, tags, metadata)

        self.extract_promoted_contexts(processed, contexts, tags)

        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            contexts
        )
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        exception: SentryExceptionChain = (
            # XXX(markus): pretty sure there should be no
            # sentry.interfaces.Exception in the pipeline
            data.get(
                "exception",
                cast(
                    SentryExceptionChain, data.get("sentry.interfaces.Exception", None)
                ),
            )
            or {"values": []}
        )
        stacks = exception.get("values", None) or []

        threadChain: SentryThreadChain = data.get(
            "threads",
            cast(SentryThreadChain, data.get("sentry.interfaces.Threads", None)),
        ) or {"values": []}
        threads = threadChain.get("values", None) or []

        self.extract_stacktraces(processed, stacks, threads)

        processing_errors = data.get("errors", None)
        if processing_errors is None:
            processed["num_processing_errors"] = 0
        elif processing_errors is not None and isinstance(processing_errors, list):
            processed["num_processing_errors"] = len(processing_errors)

        processed["offset"] = metadata.offset
        processed["partition"] = metadata.partition
        processed["message_timestamp"] = metadata.timestamp

        return processed

    def extract_promoted_tags(
        self,
        output: MutableMapping[str, Any],
        tags: Mapping[str, Any],
    ) -> None:
        output.update(
            {
                col_name: _unicodify(tags.get(tag_name, None))
                for tag_name, col_name in self._promoted_tag_columns.items()
            }
        )

    def _should_process(self, event: InsertEvent) -> bool:
        return event["data"].get("type") != "transaction"

    def _extract_event_id(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
    ) -> None:
        output["event_id"] = str(uuid.UUID(event["event_id"]))

    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        metadata: KafkaMessageMetadata,
    ) -> MutableMapping[str, Any]:
        data = event.get("data", {})

        # XXX(markus): pretty sure there should be no
        # sentry.interfaces.User in the pipeline
        user_dict = (
            data.get("user", cast(SentryUser, data.get("sentry.interfaces.User", None)))
            or {}
        )

        user_data: MutableMapping[str, Any] = {}
        extract_user(user_data, user_dict)
        output["user_name"] = user_data["username"]
        output["user_id"] = user_data["user_id"]
        output["user_email"] = user_data["email"]

        ip_address = _ensure_valid_ip(user_data["ip_address"])
        if ip_address:
            if ip_address.version == 4:
                output["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                output["ip_address_v6"] = str(ip_address)

        contexts: MutableMapping[str, Any] = _as_dict_safe(data.get("contexts", None))
        geo = user_dict.get("geo", None) or {}
        if "geo" not in contexts and isinstance(geo, dict):
            contexts["geo"] = geo

        request = (
            data.get(
                "request", cast(SentryRequest, data.get("sentry.interfaces.Http", None))
            )
            or {}
        )
        http_data: MutableMapping[str, Any] = {}
        extract_http(http_data, request)
        output["http_method"] = http_data["http_method"]
        output["http_referer"] = http_data["http_referer"]

        output["message"] = _unicodify(event["message"])

        output["primary_hash"] = str(uuid.UUID(_hashify(event["primary_hash"])))
        output["hierarchical_hashes"] = list(
            str(uuid.UUID(_hashify(x))) for x in data.get("hierarchical_hashes") or ()
        )

        output["culprit"] = _unicodify(data.get("culprit", ""))
        output["type"] = _unicodify(data.get("type", ""))
        output["title"] = _unicodify(data.get("title", ""))

        return contexts

    def extract_tags_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        tags: Mapping[str, Any],
        metadata: KafkaMessageMetadata,
    ) -> None:
        output["release"] = tags.get("sentry:release")
        output["dist"] = tags.get("sentry:dist")
        output["user"] = tags.get("sentry:user", "") or ""

        replay_id = tags.get("replayId")
        if replay_id:
            try:
                # replay_id as a tag is not guarenteed to be UUID (user could set value in theory)
                # so simply continue if not UUID.
                output["replay_id"] = str(uuid.UUID(replay_id))
            except ValueError:
                pass

        # The table has an empty string default, but the events coming from eventstream
        # often have transaction_name set to NULL, so we need to replace that with
        # an empty string.
        output["transaction_name"] = tags.get("transaction", "") or ""

    def extract_promoted_contexts(
        self,
        output: MutableMapping[str, Any],
        contexts: MutableMapping[str, Any],
        tags: MutableMapping[str, Any],
    ) -> None:
        transaction_ctx = contexts.get("trace") or {}
        trace_id = transaction_ctx.get("trace_id", None)
        span_id = transaction_ctx.get("span_id", None)
        trace_sampled = transaction_ctx.get("sampled", None)

        replay_ctx = contexts.get("replay") or {}
        replay_id = replay_ctx.get("replay_id", None)

        if replay_id:
            replay_id_uuid = uuid.UUID(replay_id)
            output["replay_id"] = str(replay_id_uuid)
            if "replayId" not in tags:
                tags["replayId"] = replay_id_uuid.hex
            del contexts["replay"]

        if trace_id:
            output["trace_id"] = str(uuid.UUID(trace_id))
        if span_id:
            output["span_id"] = int(span_id, 16)
        if trace_sampled:
            output["trace_sampled"] = bool(trace_sampled)

    def extract_common(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        metadata: KafkaMessageMetadata,
    ) -> None:
        # Properties we get from the top level of the message payload
        output["platform"] = _unicodify(event["platform"])

        # Properties we get from the "data" dict, which is the actual event body.
        data = event.get("data", {})
        received = _collapse_uint32(int(data["received"]))
        output["received"] = (
            datetime.utcfromtimestamp(received) if received is not None else None
        )
        output["version"] = _unicodify(data.get("version", None))
        output["location"] = _unicodify(data.get("location", None))

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

    def extract_stacktraces(
        self,
        output: MutableMapping[str, Any],
        stacks: Sequence[Any],
        threads: Sequence[Any],
    ) -> None:
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
        exception_main_thread = None

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

                thread_id = stack.get("thread_id", None)

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

                ## mark if at least one of the exceptions happened in the main thread
                if thread_id is not None and exception_main_thread is not True:
                    for thread in threads:
                        if thread is None:
                            continue

                        main = thread.get("main", None)
                        id = thread.get("id", None)

                        if main is None or id is None:
                            continue

                        if id == thread_id and main is True:
                            ## if it's the main thread, mark it as such and stop it
                            exception_main_thread = True
                            break
                        else:
                            ## if it's NOT the main thread, mark it as such, but
                            ## keep looking for the main thread
                            exception_main_thread = False

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

        if exception_main_thread is not None:
            output["exception_main_thread"] = exception_main_thread

    def extract_required(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
    ) -> None:
        output["group_id"] = event["group_id"] or 0

        # This is not ideal but it should never happen anyways
        timestamp = _ensure_valid_date(
            datetime.strptime(event["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
        )
        if timestamp is None:
            timestamp = datetime.utcnow()

        output["timestamp"] = timestamp

    def extract_sdk(
        self, output: MutableMapping[str, Any], sdk: Mapping[str, Any]
    ) -> None:
        output["sdk_name"] = _unicodify(sdk.get("name", None))
        output["sdk_version"] = _unicodify(sdk.get("version", None))

        sdk_integrations = []
        for i in sdk.get("integrations", None) or ():
            i = _unicodify(i)
            if i:
                sdk_integrations.append(i)
        output["sdk_integrations"] = sdk_integrations
