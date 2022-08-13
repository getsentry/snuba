from __future__ import annotations

from typing import Any

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


def get_storage(storage_key: StorageKey) -> ReadableTableStorage | WritableTableStorage:
    """
    Builds a storage object completely from config.
    """

    config = load_storage_config(storage_key)
    schema_type = TableSchema
    storage_type: Any = ReadableTableStorage

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
        QUERY_PROCESSORS: get_query_processors(config[QUERY_PROCESSORS]),
    }

    if config[KIND] == WRITABLE_STORAGE:
        # add additional kwargs related to WritableTableStorage
        kwargs[STREAM_LOADER] = build_kafka_stream_loader_from_settings(
            processor=CONF_TO_PROCESSOR[config[STREAM_LOADER]["processor"]](),
            default_topic=Topic(config[STREAM_LOADER]["default_topic"]),
            dead_letter_queue_policy_creator=generate_policy_creator(
                config[STREAM_LOADER]["dlq_policy"]
            ),
            commit_log_topic=Topic(config[STREAM_LOADER]["commit_log_topic"]),
            subscription_scheduled_topic=Topic(
                config[STREAM_LOADER]["subscription_scheduled_topic"]
            ),
            subscription_scheduler_mode=SchedulingWatermarkMode(
                config[STREAM_LOADER]["subscription_scheduler_mode"]
            ),
            subscription_result_topic=Topic(
                config[STREAM_LOADER]["subscription_result_topic"]
            ),
            replacement_topic=Topic(config[STREAM_LOADER]["replacement_topic"])
            if config[STREAM_LOADER]["replacement_topic"]
            else None,
            pre_filter=CONF_TO_PREFILTER[config[STREAM_LOADER][PRE_FILTER]["type"]](
                *config[STREAM_LOADER][PRE_FILTER]["args"]
            ),
        )

    storage = storage_type(**kwargs)
    assert isinstance(storage, ReadableTableStorage) or isinstance(
        storage, WritableTableStorage
    )
    return storage
