from __future__ import annotations

from typing import Any

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc.cdcstorage import CdcStorage
from snuba.datasets.cdc.row_processors import CdcRowProcessor
from snuba.datasets.configuration.json_schema import STORAGE_VALIDATORS
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.utils import (
    get_mandatory_condition_checkers,
    get_query_processors,
    get_query_splitters,
    parse_columns,
)
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.storage_key import register_storage_key
from snuba.datasets.table_storage import (
    KafkaStreamLoader,
    build_kafka_stream_loader_from_settings,
)
from snuba.query.allocation_policies import AllocationPolicy
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.util import PartSegment
from snuba.utils.streams.topics import Topic

KIND = "kind"
READABLE_STORAGE = "readable_storage"
WRITABLE_STORAGE = "writable_storage"
STORAGE = "storage"
STORAGE_KEY = "storage_key"
SET_KEY = "set_key"
READINESS_STATE = "readiness_state"
SCHEMA = "schema"
STREAM_LOADER = "stream_loader"
PRE_FILTER = "pre_filter"
QUERY_PROCESSORS = "query_processors"
QUERY_SPLITTERS = "query_splitters"
MANDATORY_CONDITION_CHECKERS = "mandatory_condition_checkers"
WRITER_OPTIONS = "writer_options"
SUBCRIPTION_SCHEDULER_MODE = "subscription_scheduler_mode"
DLQ_POLICY = "dlq_policy"
REPLACER_PROCESSOR = "replacer_processor"
ALLOCATION_POLICIES = "allocation_policies"


def build_storage_from_config(
    config_file_path: str,
) -> ReadableTableStorage | WritableTableStorage:
    config = load_configuration_data(config_file_path, STORAGE_VALIDATORS)

    storage_kwargs = __build_readable_storage_kwargs(config)
    if config[KIND] == READABLE_STORAGE:
        return ReadableTableStorage(**storage_kwargs)

    storage_kwargs = {**storage_kwargs, **__build_writable_storage_kwargs(config)}
    if config[KIND] == WRITABLE_STORAGE:
        return WritableTableStorage(**storage_kwargs)

    storage_kwargs = {**storage_kwargs, **__build_cdc_storage_kwargs(config)}
    return CdcStorage(**storage_kwargs)


def __build_readable_storage_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    storage_key = register_storage_key(config[STORAGE]["key"])
    return {
        STORAGE_KEY: storage_key,
        "storage_set_key": StorageSetKey(config[STORAGE][SET_KEY]),
        SCHEMA: __build_storage_schema(config),
        READINESS_STATE: ReadinessState(config[READINESS_STATE]),
        QUERY_PROCESSORS: get_query_processors(
            config[QUERY_PROCESSORS] if QUERY_PROCESSORS in config else []
        ),
        QUERY_SPLITTERS: get_query_splitters(
            config[QUERY_SPLITTERS] if QUERY_SPLITTERS in config else []
        ),
        MANDATORY_CONDITION_CHECKERS: get_mandatory_condition_checkers(
            config[MANDATORY_CONDITION_CHECKERS]
            if MANDATORY_CONDITION_CHECKERS in config
            else []
        ),
        ALLOCATION_POLICIES: [
            AllocationPolicy.get_from_name(policy["name"]).from_kwargs(
                **{
                    **policy.get("args", {}),
                    "storage_key": storage_key.value,
                }
            )
            for policy in config[ALLOCATION_POLICIES]
        ]
        if ALLOCATION_POLICIES in config
        else [],
    }


def __build_writable_storage_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    return {
        STREAM_LOADER: build_stream_loader(config[STREAM_LOADER]),
        WRITER_OPTIONS: config[WRITER_OPTIONS] if WRITER_OPTIONS in config else {},
        REPLACER_PROCESSOR: ReplacerProcessor.get_from_name(
            config[REPLACER_PROCESSOR]["processor"]
        ).from_kwargs(**config[REPLACER_PROCESSOR].get("args", {}))
        if REPLACER_PROCESSOR in config
        else {},
        # TODO: Rest of writable storage optional args
    }


def __build_cdc_storage_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "default_control_topic": config["default_control_topic"],
        "postgres_table": config["postgres_table"],
        "row_processor": CdcRowProcessor.get_from_name(
            config["row_processor"]["processor"]
        ).from_kwargs(**config["row_processor"].get("args", {})),
    }


def __build_storage_schema(config: dict[str, Any]) -> TableSchema:
    schema_class = (
        TableSchema if config[KIND] == READABLE_STORAGE else WritableTableSchema
    )
    partition_formats = None
    if "partition_format" in config[SCHEMA]:
        partition_formats = []
        for pformat in config[SCHEMA]["partition_format"]:
            for partition_format in PartSegment:
                if pformat == partition_format.value:
                    partition_formats.append(partition_format)

    mandatory_conditions = None
    if "not_deleted_mandatory_condition" in config[SCHEMA]:
        deletion_flag_column = config[SCHEMA]["not_deleted_mandatory_condition"]
        mandatory_conditions = [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, deletion_flag_column),
                Literal(None, 0),
            )
        ]

    return schema_class(
        columns=ColumnSet(parse_columns(config[SCHEMA]["columns"])),
        local_table_name=config[SCHEMA]["local_table_name"],
        dist_table_name=config[SCHEMA]["dist_table_name"],
        storage_set_key=StorageSetKey(config[STORAGE][SET_KEY]),
        mandatory_conditions=mandatory_conditions,
        partition_format=partition_formats,
    )


def build_stream_loader(loader_config: dict[str, Any]) -> KafkaStreamLoader:
    processor_name = loader_config["processor"]
    processor = DatasetMessageProcessor.from_name(processor_name)
    assert processor is not None
    default_topic = Topic(loader_config["default_topic"])
    # optionals
    pre_filter = None
    if PRE_FILTER in loader_config and loader_config[PRE_FILTER] is not None:
        pre_filter = StreamMessageFilter.get_from_name(
            loader_config[PRE_FILTER]["type"]
        ).from_kwargs(**loader_config[PRE_FILTER].get("args", {}))
    replacement_topic = __get_topic(loader_config, "replacement_topic")
    commit_log_topic = __get_topic(loader_config, "commit_log_topic")
    subscription_scheduled_topic = __get_topic(
        loader_config, "subscription_scheduled_topic"
    )
    subscription_scheduler_mode = (
        SchedulingWatermarkMode(loader_config[SUBCRIPTION_SCHEDULER_MODE])
        if SUBCRIPTION_SCHEDULER_MODE in loader_config
        and loader_config[SUBCRIPTION_SCHEDULER_MODE] is not None
        else None
    )
    subscription_result_topic = __get_topic(loader_config, "subscription_result_topic")

    dlq_topic = __get_topic(loader_config, "dlq_topic")

    return build_kafka_stream_loader_from_settings(
        processor,
        default_topic,
        pre_filter,
        replacement_topic,
        commit_log_topic,
        subscription_scheduler_mode,
        subscription_scheduled_topic,
        subscription_result_topic,
        dlq_topic,
    )


def __get_topic(stream_loader_config: dict[str, Any], name: str | None) -> Topic | None:
    return (
        Topic(stream_loader_config[name])
        if name in stream_loader_config and stream_loader_config[name] is not None
        else None
    )
