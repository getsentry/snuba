from __future__ import annotations

from typing import Any

from jsonschema import validate
from yaml import safe_load

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.configuration.json_schema import (
    V1_READABLE_STORAGE_SCHEMA,
    V1_WRITABLE_STORAGE_SCHEMA,
)
from snuba.datasets.configuration.utils import (
    CONF_TO_PREFILTER,
    CONF_TO_PROCESSOR,
    generate_policy_creator,
    get_query_processors,
    parse_columns,
)
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import (
    KafkaStreamLoader,
    build_kafka_stream_loader_from_settings,
)
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

# TODO: The config files should be discovered automatically
CONFIG_FILES_PATH = "./snuba/datasets/configuration/generic_metrics/storages"
CONFIG_FILES = {
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS: f"{CONFIG_FILES_PATH}/distributions.yaml",
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW: f"{CONFIG_FILES_PATH}/distributions_bucket.yaml",
    StorageKey.GENERIC_METRICS_SETS_RAW: f"{CONFIG_FILES_PATH}/sets_bucket.yaml",
    StorageKey.GENERIC_METRICS_SETS: f"{CONFIG_FILES_PATH}/sets.yaml",
}

STORAGE_VALIDATION_SCHEMAS = {
    "readonly_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
}


def build_readonly_storage(storage_key: StorageKey) -> ReadableTableStorage:
    config = __load_storage_config(storage_key)
    storage_kwargs = __build_readonly_storage_kwargs(config)
    return ReadableTableStorage(**storage_kwargs)


def build_writable_storage(storage_key: StorageKey) -> WritableTableStorage:
    config = __load_storage_config(storage_key)
    storage_kwargs = __build_readonly_storage_kwargs(config)
    storage_kwargs[STREAM_LOADER] = __build_stream_loader(config[STREAM_LOADER])
    return WritableTableStorage(**storage_kwargs)


def __load_storage_config(storage_key: StorageKey) -> dict[str, Any]:
    file = open(CONFIG_FILES[storage_key])
    config = safe_load(file)
    assert isinstance(config, dict)
    validate(config, STORAGE_VALIDATION_SCHEMAS[config["kind"]])
    return config


def __build_readonly_storage_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    return {
        STORAGE_KEY: StorageKey(config[STORAGE]["key"]),
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
    }


def __build_stream_loader(loader_config: dict[str, Any]) -> KafkaStreamLoader:

    processor = CONF_TO_PROCESSOR[loader_config["processor"]]()
    default_topic = Topic(loader_config["default_topic"])

    # optionals
    pre_filter = (
        CONF_TO_PREFILTER[loader_config[PRE_FILTER]["type"]](
            *loader_config[PRE_FILTER]["args"]
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
