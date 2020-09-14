from snuba.clickhouse.columns import Array, ColumnSet, DateTime, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.datasets.metrics_processor import MetricsProcessor

columns = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("name", String()),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("tags_hash", Array(UInt(64))),
        ("count", UInt(64)),
        ("quantiles_sketch", Array(UInt(8))),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="metrics_local",
    dist_table_name="metrics_dist",
    storage_set_key=StorageSetKey.METRICS,
)

storage = WritableTableStorage(
    storage_key=StorageKey.METRICS,
    storage_set_key=StorageSetKey.METRICS,
    schema=schema,
    query_processors=[],
    stream_loader=KafkaStreamLoader(
        processor=MetricsProcessor(), default_topic="metrics-aggregator",
    ),
)
