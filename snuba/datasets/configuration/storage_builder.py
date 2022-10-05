from __future__ import annotations

from typing import Any

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.configuration.json_schema import (
    V1_READABLE_STORAGE_SCHEMA,
    V1_WRITABLE_STORAGE_SCHEMA,
)
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.utils import (
    CONF_TO_PREFILTER,
    generate_policy_creator,
    get_query_processors,
    parse_columns,
)
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.storage_key import register_storage_key
from snuba.datasets.table_storage import (
    KafkaStreamLoader,
    build_kafka_stream_loader_from_settings,
)
from snuba.query.processors.physical import ClickhouseQueryProcessor
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


STORAGE_VALIDATION_SCHEMAS = {
    "readable_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
}


def build_storage(
    config_file_path: str,
) -> ReadableTableStorage | WritableTableStorage:
    config = load_configuration_data(config_file_path, STORAGE_VALIDATION_SCHEMAS)
    storage_kwargs = __build_readable_storage_kwargs(config)
    if config[KIND] == "readable_storage":
        return ReadableTableStorage(**storage_kwargs)
    storage_kwargs[STREAM_LOADER] = build_stream_loader(config[STREAM_LOADER])
    return WritableTableStorage(**storage_kwargs)


def __build_readable_storage_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    return {
        STORAGE_KEY: register_storage_key(config[STORAGE]["key"]),
        "storage_set_key": StorageSetKey(config[STORAGE][SET_KEY]),
        SCHEMA: (
            WritableTableSchema if config[KIND] == WRITABLE_STORAGE else TableSchema
        )(
            columns=ColumnSet(parse_columns(config[SCHEMA]["columns"])),
            local_table_name=config[SCHEMA]["local_table_name"],
            dist_table_name=config[SCHEMA]["dist_table_name"],
            storage_set_key=StorageSetKey(config[STORAGE][SET_KEY]),
        ),
        QUERY_PROCESSORS: get_query_processors(
            config[QUERY_PROCESSORS] if QUERY_PROCESSORS in config else []
        ),
        # TODO: Rest of readable storage optional args
    }


def build_stream_loader(loader_config: dict[str, Any]) -> KafkaStreamLoader:
    processor = ClickhouseQueryProcessor.get_from_name(
        loader_config["processor"]
    ).from_kwargs()
    default_topic = Topic(loader_config["default_topic"])
    # optionals
    pre_filter = (
        CONF_TO_PREFILTER[loader_config[PRE_FILTER]["type"]](
            **loader_config[PRE_FILTER]["args"]
        )
        if PRE_FILTER in loader_config and loader_config[PRE_FILTER] is not None
        else None
    )
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
    dead_letter_queue_policy_creator = (
        generate_policy_creator(loader_config[DLQ_POLICY])
        if DLQ_POLICY in loader_config and loader_config[DLQ_POLICY] is not None
        else None
    )

    return build_kafka_stream_loader_from_settings(
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


def __get_topic(stream_loader_config: dict[str, Any], name: str | None) -> Topic | None:
    return (
        Topic(stream_loader_config[name])
        if name in stream_loader_config and stream_loader_config[name] is not None
        else None
    )
