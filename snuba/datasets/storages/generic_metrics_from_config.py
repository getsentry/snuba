"""
The storages defined in this file are for the generic metrics system,
initially built to handle metrics-enhanced performance.
"""


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

conf_sets_bucket = load_storage_config(StorageKey.GENERIC_METRICS_SETS_RAW)

sets_bucket_storage = WritableTableStorage(
    storage_key=StorageKey(conf_sets_bucket["storage"]["key"]),
    storage_set_key=StorageSetKey(conf_sets_bucket["storage"]["set_key"]),
    schema=WritableTableSchema(
        columns=ColumnSet(parse_columns(conf_sets_bucket["schema"]["columns"])),
        local_table_name=conf_sets_bucket["schema"]["local_table_name"],
        dist_table_name=conf_sets_bucket["schema"]["dist_table_name"],
        storage_set_key=StorageSetKey(conf_sets_bucket["storage"]["set_key"]),
    ),
    query_processors=conf_sets_bucket["query_processors"],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=CONF_TO_PROCESSOR[conf_sets_bucket["stream_loader"]["processor"]](),
        default_topic=Topic(conf_sets_bucket["stream_loader"]["default_topic"]),
        dead_letter_queue_policy_creator=generate_policy_creator(
            conf_sets_bucket["stream_loader"]["dlq_policy"]
        ),
        commit_log_topic=Topic(conf_sets_bucket["stream_loader"]["commit_log_topic"]),
        subscription_scheduled_topic=Topic(
            conf_sets_bucket["stream_loader"]["subscription_scheduled_topic"]
        ),
        subscription_scheduler_mode=SchedulingWatermarkMode(
            conf_sets_bucket["stream_loader"]["subscription_scheduler_mode"]
        ),
        subscription_result_topic=Topic(
            conf_sets_bucket["stream_loader"]["subscription_result_topic"]
        ),
        replacement_topic=Topic(conf_sets_bucket["stream_loader"]["replacement_topic"])
        if conf_sets_bucket["stream_loader"]["replacement_topic"]
        else None,
        pre_filter=CONF_TO_PREFILTER[
            conf_sets_bucket["stream_loader"]["pre_filter"]["type"]
        ](*conf_sets_bucket["stream_loader"]["pre_filter"]["args"]),
    ),
)

conf_distributions = load_storage_config(StorageKey.GENERIC_METRICS_DISTRIBUTIONS)

distributions_storage = ReadableTableStorage(
    storage_key=StorageKey(conf_distributions["storage"]["key"]),
    storage_set_key=StorageSetKey(conf_distributions["storage"]["set_key"]),
    schema=TableSchema(
        local_table_name="generic_metric_distributions_aggregated_local",
        dist_table_name="generic_metric_distributions_aggregated_dist",
        storage_set_key=StorageSetKey(conf_distributions["storage"]["set_key"]),
        columns=ColumnSet(parse_columns(conf_distributions["schema"]["columns"])),
    ),
    query_processors=get_query_processors(conf_distributions["query_processors"]),
)

conf_distributions_bucket = load_storage_config(
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW
)

distributions_bucket_storage = WritableTableStorage(
    storage_key=StorageKey(conf_distributions_bucket["storage"]["key"]),
    storage_set_key=StorageSetKey(conf_distributions_bucket["storage"]["set_key"]),
    schema=WritableTableSchema(
        columns=ColumnSet(
            parse_columns(conf_distributions_bucket["schema"]["columns"])
        ),
        local_table_name=conf_distributions_bucket["schema"]["local_table_name"],
        dist_table_name=conf_distributions_bucket["schema"]["dist_table_name"],
        storage_set_key=StorageSetKey(conf_distributions_bucket["storage"]["set_key"]),
    ),
    query_processors=conf_distributions_bucket["query_processors"],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=CONF_TO_PROCESSOR[
            conf_distributions_bucket["stream_loader"]["processor"]
        ](),
        default_topic=Topic(
            conf_distributions_bucket["stream_loader"]["default_topic"]
        ),
        dead_letter_queue_policy_creator=generate_policy_creator(
            conf_distributions_bucket["stream_loader"]["dlq_policy"]
        ),
        commit_log_topic=Topic(
            conf_distributions_bucket["stream_loader"]["commit_log_topic"]
        ),
        subscription_scheduled_topic=Topic(
            conf_distributions_bucket["stream_loader"]["subscription_scheduled_topic"]
        ),
        subscription_scheduler_mode=SchedulingWatermarkMode(
            conf_distributions_bucket["stream_loader"]["subscription_scheduler_mode"]
        ),
        subscription_result_topic=Topic(
            conf_distributions_bucket["stream_loader"]["subscription_result_topic"]
        ),
        replacement_topic=Topic(
            conf_distributions_bucket["stream_loader"]["replacement_topic"]
        )
        if conf_distributions_bucket["stream_loader"]["replacement_topic"]
        else None,
        pre_filter=CONF_TO_PREFILTER[
            conf_distributions_bucket["stream_loader"]["pre_filter"]["type"]
        ](*conf_distributions_bucket["stream_loader"]["pre_filter"]["args"]),
    ),
)
