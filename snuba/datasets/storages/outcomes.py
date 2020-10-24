from snuba.clickhouse.columns import (
    UUID,
    ColumnSet,
    DateTime,
    SchemaModifiers,
    String,
    UInt,
    nullable,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.outcomes_processor import OutcomesProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.prewhere import PrewhereProcessor

WRITE_LOCAL_TABLE_NAME = "outcomes_raw_local"
WRITE_DIST_TABLE_NAME = "outcomes_raw_dist"
READ_LOCAL_TABLE_NAME = "outcomes_hourly_local"
READ_DIST_TABLE_NAME = "outcomes_hourly_dist"

write_columns = ColumnSet[SchemaModifiers](
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64, nullable())),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", String(nullable())),
        ("event_id", UUID(nullable())),
    ]
)

raw_schema = WritableTableSchema(
    columns=write_columns,
    # TODO: change to outcomes.raw_local when we add multi DB support
    local_table_name=WRITE_LOCAL_TABLE_NAME,
    dist_table_name=WRITE_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
)

read_columns = ColumnSet[SchemaModifiers](
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64)),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", String()),
        ("times_seen", UInt(64)),
    ]
)

read_schema = TableSchema(
    columns=read_columns,
    local_table_name=READ_LOCAL_TABLE_NAME,
    dist_table_name=READ_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
)

materialized_view_columns = ColumnSet[SchemaModifiers](
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64)),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", String()),
        ("times_seen", UInt(64)),
    ]
)

materialized_view_schema = TableSchema(
    local_table_name="outcomes_mv_hourly_local",
    dist_table_name="outcomes_mv_hourly_dist",
    storage_set_key=StorageSetKey.OUTCOMES,
    prewhere_candidates=["project_id", "org_id"],
    columns=materialized_view_columns,
)

raw_storage = WritableTableStorage(
    storage_key=StorageKey.OUTCOMES_RAW,
    storage_set_key=StorageSetKey.OUTCOMES,
    schema=raw_schema,
    query_processors=[],
    stream_loader=KafkaStreamLoader(
        processor=OutcomesProcessor(), default_topic="outcomes",
    ),
)

materialized_storage = ReadableTableStorage(
    storage_key=StorageKey.OUTCOMES_HOURLY,
    storage_set_key=StorageSetKey.OUTCOMES,
    schema=read_schema,
    query_processors=[PrewhereProcessor()],
)
