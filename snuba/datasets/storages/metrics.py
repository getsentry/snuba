from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.outcomes_processor import OutcomesProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings

buckets_columns = ColumnSet([])

buckets_schema = WritableTableSchema(
    columns=buckets_columns,
    # TODO: change to outcomes.raw_local when we add multi DB support
    local_table_name="metrics_buckets_local",
    dist_table_name="metrics_buckets_dist",
    storage_set_key=StorageSetKey.METRICS,
)

buckets_storage = WritableTableStorage(
    storage_key=StorageKey.METRICS_BUCKETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=buckets_schema,
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        StorageKey.OUTCOMES_RAW,
        processor=OutcomesProcessor(),
        default_topic_name="metrics",
    ),
)

sets_columns = ColumnSet([])

sets_aggregated_schema = TableSchema(
    local_table_name="metrics_sets_local",
    dist_table_name="metrics_sets_dist",
    storage_set_key=StorageSetKey.METRICS,
    columns=sets_columns,
)

sets_storage = ReadableTableStorage(
    storage_key=StorageKey.METRICS_SETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=sets_aggregated_schema,
    query_processors=[],
)
