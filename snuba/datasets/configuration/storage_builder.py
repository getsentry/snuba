from __future__ import annotations

from typing import Any, Type

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.configuration.utils import (
    CONF_TO_PREFILTER,
    CONF_TO_PROCESSOR,
    generate_policy_creator,
    get_query_processors,
    load_storage_config,
    parse_columns,
)
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic

KIND = "kind"
WRITABLE_STORAGE = "writable_storage"
STORAGE = "storage"
STORAGE_KEY = "storage_key"
SET_KEY = "set_key"
SCHEMA = "schema"
STREAM_LOADER = "stream_loader"
PRE_FILTER = "pre_filter"
QUERY_PROCESSORS = "query_processors"
SUBCRIPTION_SCHEDULER_MODE = "subscription_scheduler_mode"
DLQ_POLICY = "dlq_policy"


def get_storage(storage_key: StorageKey) -> ReadableTableStorage | WritableTableStorage:
    """
    Builds a storage object completely from config.
    """

    config = load_storage_config(storage_key)
    schema_type = TableSchema
    storage_type: Type[
        ReadableTableStorage | WritableTableStorage
    ] = ReadableTableStorage

    if config[KIND] == WRITABLE_STORAGE:
        schema_type = WritableTableSchema
        storage_type = WritableTableStorage

    # set up kwargs for ReadableTableStorage
    kwargs = {
        STORAGE_KEY: storage_key,
        "storage_set_key": StorageSetKey(config[STORAGE][SET_KEY]),
        SCHEMA: schema_type(
            columns=ColumnSet(parse_columns(config[SCHEMA]["columns"])),
            local_table_name=config[SCHEMA]["local_table_name"],
            dist_table_name=config[SCHEMA]["dist_table_name"],
            storage_set_key=StorageSetKey(config[STORAGE][SET_KEY]),
        ),
        QUERY_PROCESSORS: get_query_processors(
            config[QUERY_PROCESSORS] if QUERY_PROCESSORS in config else []
        ),
    }

    if config[KIND] == WRITABLE_STORAGE:
        sl_config = config[STREAM_LOADER]
        processor = CONF_TO_PROCESSOR[sl_config["processor"]]()
        default_topic = Topic(sl_config["default_topic"])

        # optionals
        pre_filter = (
            CONF_TO_PREFILTER[sl_config[PRE_FILTER]["type"]](
                *sl_config[PRE_FILTER]["args"]
            )
            if PRE_FILTER in sl_config and sl_config[PRE_FILTER] is not None
            else None
        )
        replacement_topic = __get_topic(sl_config, "replacement_topic")
        commit_log_topic = __get_topic(sl_config, "commit_log_topic")
        subscription_scheduled_topic = __get_topic(
            sl_config, "subscription_scheduled_topic"
        )
        subscription_scheduler_mode = (
            SchedulingWatermarkMode(sl_config[SUBCRIPTION_SCHEDULER_MODE])
            if SUBCRIPTION_SCHEDULER_MODE in sl_config
            and sl_config[SUBCRIPTION_SCHEDULER_MODE] is not None
            else None
        )
        subscription_result_topic = __get_topic(sl_config, "subscription_result_topic")
        dead_letter_queue_policy_creator = (
            generate_policy_creator(sl_config[DLQ_POLICY])
            if DLQ_POLICY in sl_config and sl_config[DLQ_POLICY] is not None
            else None
        )

        # add additional kwargs related to WritableTableStorage
        kwargs[STREAM_LOADER] = build_kafka_stream_loader_from_settings(
            processor,
            default_topic,
            pre_filter,
            replacement_topic,
            commit_log_topic,
            subscription_scheduler_mode,
            subscription_scheduled_topic,
            subscription_result_topic,
            dead_letter_queue_policy_creator,
        )

    storage = storage_type(**kwargs)  # type: ignore
    return storage


def __get_topic(stream_loader_config: dict[str, Any], name: str | None) -> Topic | None:
    return (
        Topic(stream_loader_config[name])
        if name in stream_loader_config and stream_loader_config[name] is not None
        else None
    )
