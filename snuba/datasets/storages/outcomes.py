from snuba.clickhouse.columns import (
    UUID,
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.outcomes_processor import OutcomesProcessor
from snuba.datasets.schemas.tables import (
    MaterializedViewSchema,
    MergeTreeSchema,
    SummingMergeTreeSchema,
)
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.expressions import Column
from snuba.query.processors.prewhere import PrewhereProcessor

WRITE_LOCAL_TABLE_NAME = "outcomes_raw_local"
WRITE_DIST_TABLE_NAME = "outcomes_raw_dist"
READ_LOCAL_TABLE_NAME = "outcomes_hourly_local"
READ_DIST_TABLE_NAME = "outcomes_hourly_dist"


def col(name: str) -> Column:
    return Column(None, None, name)


write_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", Nullable(UInt(64))),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", LowCardinality(Nullable(String()))),
        ("event_id", Nullable(UUID())),
    ]
)

raw_schema = MergeTreeSchema(
    columns=write_columns,
    # TODO: change to outcomes.raw_local when we add multi DB support
    local_table_name=WRITE_LOCAL_TABLE_NAME,
    dist_table_name=WRITE_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
    order_by=[col("org_id"), col("project_id"), col("timestamp")],
    partition_by="(toMonday(timestamp))",
    settings={"index_granularity": "16384"},
)

read_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64)),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", LowCardinality(String())),
        ("times_seen", UInt(64)),
    ]
)

read_schema = SummingMergeTreeSchema(
    columns=read_columns,
    local_table_name=READ_LOCAL_TABLE_NAME,
    dist_table_name=READ_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
    order_by=[
        col("org_id"),
        col("project_id"),
        col("key_id"),
        col("outcome"),
        col("reason"),
        col("timestamp"),
    ],
    partition_by="(toMonday(timestamp))",
    settings={"index_granularity": "256"},
)

materialized_view_columns = ColumnSet(
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

# TODO: Find a better way to specify a query for a materialized view
# The problem right now is that we have a way to define our columns in a ColumnSet abstraction but the query
# doesn't use it.
query = """
        SELECT
            org_id,
            project_id,
            ifNull(key_id, 0) AS key_id,
            toStartOfHour(timestamp) AS timestamp,
            outcome,
            ifNull(reason, 'none') AS reason,
            count() AS times_seen
        FROM %(source_table_name)s
        GROUP BY org_id, project_id, key_id, timestamp, outcome, reason
        """

materialized_view_schema = MaterializedViewSchema(
    local_materialized_view_name="outcomes_mv_hourly_local",
    dist_materialized_view_name="outcomes_mv_hourly_dist",
    storage_set_key=StorageSetKey.OUTCOMES,
    prewhere_candidates=["project_id", "org_id"],
    columns=materialized_view_columns,
    query=query,
    local_source_table_name=WRITE_LOCAL_TABLE_NAME,
    local_destination_table_name=READ_LOCAL_TABLE_NAME,
    dist_source_table_name=WRITE_DIST_TABLE_NAME,
    dist_destination_table_name=READ_DIST_TABLE_NAME,
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
