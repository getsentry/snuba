from __future__ import annotations

from enum import Enum
from typing import Any, Type

from snuba.datasets.configuration.utils import (
    CONF_TO_PREFILTER,
    CONF_TO_PROCESSOR,
    QUERY_PROCESSORS,
)
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic


class InvalidSemanticsError(Exception):
    pass


def validate_storage_semantics(config: dict[str, Any]) -> None:
    validate_storage_schema_semantics(config["schema"])
    if "query_processors" in config:
        validate_storage_query_processors_semantics(config["query_processors"])
    if "stream_loader" in config:
        validate_stream_loader_semantics(config["stream_loader"])


def validate_storage_schema_semantics(schema: dict[str, Any]) -> None:
    # TODO: Schema semantic validation
    pass


def validate_storage_query_processors_semantics(query_processors: list[str]) -> None:
    for processor in query_processors:
        if processor not in QUERY_PROCESSORS:
            __raise(processor, "Query Processor")


def validate_stream_loader_semantics(stream_loader: dict[str, Any]) -> None:
    if (_processor := stream_loader["processor"]) not in CONF_TO_PROCESSOR:
        __raise(_processor, "Message Processor")

    __validate_enum(stream_loader["default_topic"], Topic)

    if "pre_filter" in stream_loader:
        if (_type := stream_loader["pre_filter"]["type"]) not in CONF_TO_PREFILTER:
            __raise(_type, "Message Filter")

    if "dlq_policy" in stream_loader:
        if stream_loader["dlq_policy"]["type"] != "produce":
            __raise(stream_loader["dlq_policy"]["type"], "DLQ Policy")
        __validate_enum(stream_loader["dlq_policy"]["args"][0], Topic)

    __validate_optional_enums(
        stream_loader,
        {
            "commit_log_topic": Topic,
            "replacement_topic": Topic,
            "subscription_scheduler_mode": SchedulingWatermarkMode,
            "subscription_scheduled_topic": Topic,
            "subscription_result_topic": Topic,
        },
    )


def __validate_optional_enums(
    stream_loader: dict[str, Any], config_enums: dict[str, Type[Enum]]
) -> None:
    for key, type in config_enums.items():
        if key in stream_loader:
            __validate_enum(stream_loader[key], type)


def __validate_enum(value: str, type: Type[Enum]) -> None:
    try:
        type(value)
    except Exception:
        __raise(value, type.__name__)


def __raise(config_item: str, type: str) -> None:
    raise InvalidSemanticsError(f"{config_item} is not a valid {type}")
