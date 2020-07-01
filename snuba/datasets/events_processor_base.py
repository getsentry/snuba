import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Sequence

from typing_extensions import TypedDict

from snuba import settings
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_contexts,
    extract_extra_tags,
    extract_project_id,
)
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    MessageProcessor,
    ProcessedMessage,
    ReplacementBatch,
    _as_dict_safe,
    _boolify,
    _collapse_uint32,
    _ensure_valid_date,
    _hashify,
    _unicodify,
)

logger = logging.getLogger(__name__)


REPLACEMENT_EVENT_TYPES = frozenset(
    [
        "start_delete_groups",
        "start_merge",
        "start_unmerge",
        "start_delete_tag",
        "end_delete_groups",
        "end_merge",
        "end_unmerge",
        "end_delete_tag",
    ]
)


class InsertEvent(TypedDict):
    group_id: Optional[int]
    event_id: str
    organization_id: int
    project_id: int
    message: str
    platform: str
    datetime: str  # snuba.settings.PAYLOAD_DATETIME_FORMAT
    data: Mapping[str, Any]
    primary_hash: str  # empty string represents None
    retention_days: int


class EventsProcessorBase(MessageProcessor, ABC):
    """
    Base class for events and errors processors.
    """

    @abstractmethod
    def _should_process(self, event: InsertEvent) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _extract_event_id(
        self, output: MutableMapping[str, Any], event: InsertEvent,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def extract_promoted_tags(
        self, output: MutableMapping[str, Any], tags: Mapping[str, Any],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def extract_tags_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        tags: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def extract_promoted_contexts(
        self,
        output: MutableMapping[str, Any],
        contexts: Mapping[str, Any],
        tags: Mapping[str, Any],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def extract_contexts_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        contexts: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        raise NotImplementedError

    def extract_required(
        self, output: MutableMapping[str, Any], event: InsertEvent,
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

    def process_message(
        self, message, metadata: Optional[KafkaMessageMetadata] = None
    ) -> Optional[ProcessedMessage]:
        """\
        Process a raw message into a tuple of (action_type, processed_message):
        * action_type: one of the sentinel values INSERT or REPLACE
        * processed_message: dict representing the processed column -> value(s)
        Returns `None` if the event is too old to be written.
        """
        version = message[0]
        if version != 2:
            raise InvalidMessageVersion(f"Unsupported message version: {version}")

        # version 2: (2, type, data, [state])
        type_, event = message[1:3]
        if type_ == "insert":
            try:
                row = self.process_insert(event, metadata)
            except EventTooOld:
                return None

            if row is None:  # the processor cannot/does not handle this input
                return None

            return InsertBatch([row])
        elif type_ in REPLACEMENT_EVENT_TYPES:
            # pass raw events along to republish
            return ReplacementBatch(str(event["project_id"]), [message])
        else:
            raise InvalidMessageType(f"Invalid message type: {type_}")

    def process_insert(
        self, event: InsertEvent, metadata: Optional[KafkaMessageMetadata] = None
    ) -> Optional[Mapping[str, Any]]:
        if not self._should_process(event):
            return None

        processed = {"deleted": 0}
        extract_project_id(processed, event)
        self._extract_event_id(processed, event)
        processed["retention_days"] = enforce_retention(
            event,
            datetime.strptime(event["datetime"], settings.PAYLOAD_DATETIME_FORMAT),
        )

        self.extract_required(processed, event)

        data = event.get("data", {})
        # HACK: https://sentry.io/sentry/snuba/issues/802102397/
        if not data:
            logger.error("No data for event: %s", event, exc_info=True)
            return None
        self.extract_common(processed, event, metadata)
        self.extract_custom(processed, event, metadata)

        sdk = data.get("sdk", None) or {}
        self.extract_sdk(processed, sdk)

        tags = _as_dict_safe(data.get("tags", None))
        self.extract_promoted_tags(processed, tags)
        self.extract_tags_custom(processed, event, tags, metadata)

        contexts = data.get("contexts", None) or {}
        self.extract_promoted_contexts(processed, contexts, tags)
        self.extract_contexts_custom(processed, event, contexts, metadata)

        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            contexts
        )
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
        processed["_tags_flattened"] = ""

        exception = (
            data.get("exception", data.get("sentry.interfaces.Exception", None)) or {}
        )
        stacks = exception.get("values", None) or []
        self.extract_stacktraces(processed, stacks)

        if metadata is not None:
            processed["offset"] = metadata.offset
            processed["partition"] = metadata.partition
            processed["message_timestamp"] = metadata.timestamp

        return processed

    def extract_common(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        # Properties we get from the top level of the message payload
        output["platform"] = _unicodify(event["platform"])
        output["primary_hash"] = _hashify(event["primary_hash"])

        # Properties we get from the "data" dict, which is the actual event body.
        data = event.get("data", {})
        received = _collapse_uint32(int(data["received"]))
        output["received"] = (
            datetime.utcfromtimestamp(received) if received is not None else None
        )

        output["culprit"] = _unicodify(data.get("culprit", None))
        output["type"] = _unicodify(data.get("type", None))
        output["version"] = _unicodify(data.get("version", None))
        output["title"] = _unicodify(data.get("title", None))
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
        self, output: MutableMapping[str, Any], stacks: Sequence[Any]
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
