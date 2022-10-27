from snuba import util
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.processors.transactions_processor import (
    TransactionsMessageProcessor,
)
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.storages.transactions_common import (
    columns,
    mandatory_condition_checkers,
    query_processors,
    query_splitters,
)
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic

schema = WritableTableSchema(
    columns=columns,
    local_table_name="transactions_local",
    dist_table_name="transactions_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    partition_format=[util.PartSegment.RETENTION_DAYS, util.PartSegment.DATE],
)

storage = WritableTableStorage(
    storage_key=StorageKey.TRANSACTIONS,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=query_processors,
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=TransactionsMessageProcessor(),
        default_topic=Topic.TRANSACTIONS,
        commit_log_topic=Topic.TRANSACTIONS_COMMIT_LOG,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_TRANSACTIONS,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_TRANSACTIONS,
    ),
    query_splitters=query_splitters,
    mandatory_condition_checkers=mandatory_condition_checkers,
    writer_options={
        "insert_allow_materialized_columns": 1,
        "input_format_skip_unknown_fields": 1,
    },
)
