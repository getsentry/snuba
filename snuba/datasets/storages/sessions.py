from snuba.clickhouse.columns import (
    AggregateFunction,
    ColumnSet,
    DateTime,
    LowCardinality,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.schemas.tables import (
    MergeTreeSchema,
    MaterializedViewSchema,
    AggregatingMergeTreeSchema,
)
from snuba.datasets.sessions_processor import SessionsProcessor
from snuba.datasets.storage import (
    ReadableTableStorage,
    WritableTableStorage,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.processor import MAX_UINT32, NIL_UUID
from snuba.query.processors.prewhere import PrewhereProcessor


WRITE_LOCAL_TABLE_NAME = "sessions_raw_local"
WRITE_DIST_TABLE_NAME = "sessions_raw_dist"
READ_LOCAL_TABLE_NAME = "sessions_hourly_local"
READ_DIST_TABLE_NAME = "sessions_hourly_dist"
READ_LOCAL_MV_NAME = "sessions_hourly_mv_local"
READ_DIST_MV_NAME = "sessions_hourly_mv_dist"


all_columns = ColumnSet(
    [
        ("session_id", UUID()),
        ("distinct_id", UUID()),
        ("seq", UInt(64)),
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("retention_days", UInt(16)),
        ("duration", UInt(32)),
        ("status", UInt(8)),
        ("errors", UInt(16)),
        ("received", DateTime()),
        ("started", DateTime()),
        ("release", LowCardinality(String())),
        ("environment", LowCardinality(String())),
    ]
)

raw_schema = MergeTreeSchema(
    columns=all_columns,
    local_table_name=WRITE_LOCAL_TABLE_NAME,
    dist_table_name=WRITE_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
    order_by="(org_id, project_id, release, environment, started)",
    partition_by="(toMonday(started))",
    settings={"index_granularity": "16384"},
)

read_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("started", DateTime()),
        ("release", LowCardinality(String())),
        ("environment", LowCardinality(String())),
        (
            "duration_quantiles",
            AggregateFunction("quantilesIf(0.5, 0.9)", UInt(32), UInt(8)),
        ),
        ("sessions", AggregateFunction("countIf", UUID(), UInt(8))),
        ("users", AggregateFunction("uniqIf", UUID(), UInt(8))),
        ("sessions_crashed", AggregateFunction("countIf", UUID(), UInt(8)),),
        ("sessions_abnormal", AggregateFunction("countIf", UUID(), UInt(8)),),
        ("sessions_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
        ("users_crashed", AggregateFunction("uniqIf", UUID(), UInt(8))),
        ("users_abnormal", AggregateFunction("uniqIf", UUID(), UInt(8))),
        ("users_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
    ]
)
read_schema = AggregatingMergeTreeSchema(
    columns=read_columns,
    local_table_name=READ_LOCAL_TABLE_NAME,
    dist_table_name=READ_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
    prewhere_candidates=["project_id", "org_id"],
    order_by="(org_id, project_id, release, environment, started)",
    partition_by="(toMonday(started))",
    settings={"index_granularity": "256"},
)
materialized_view_schema = MaterializedViewSchema(
    local_materialized_view_name=READ_LOCAL_MV_NAME,
    dist_materialized_view_name=READ_DIST_MV_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
    prewhere_candidates=["project_id", "org_id"],
    columns=read_columns,
    query=f"""
        SELECT
            org_id,
            project_id,
            toStartOfHour(started) as started,
            release,
            environment,
            quantilesIfState(0.5, 0.9)(
                duration,
                duration <> {MAX_UINT32} AND status == 1
            ) as duration_quantiles,
            countIfState(session_id, seq == 0) as sessions,
            uniqIfState(distinct_id, distinct_id != '{NIL_UUID}') as users,
            countIfState(session_id, status == 2) as sessions_crashed,
            countIfState(session_id, status == 3) as sessions_abnormal,
            uniqIfState(session_id, errors > 0) as sessions_errored,
            uniqIfState(distinct_id, status == 2) as users_crashed,
            uniqIfState(distinct_id, status == 3) as users_abnormal,
            uniqIfState(distinct_id, errors > 0) as users_errored
        FROM
            %(source_table_name)s
        GROUP BY
            org_id, project_id, started, release, environment
    """,
    local_source_table_name=WRITE_LOCAL_TABLE_NAME,
    local_destination_table_name=READ_LOCAL_TABLE_NAME,
    dist_source_table_name=WRITE_DIST_TABLE_NAME,
    dist_destination_table_name=READ_DIST_TABLE_NAME,
)

# The raw table we write onto, and that potentially we could
# query.
raw_storage = WritableTableStorage(
    storage_key=StorageKey.SESSIONS_RAW,
    storage_set_key=StorageSetKey.SESSIONS,
    schemas=StorageSchemas(read_schema=raw_schema, write_schema=raw_schema),
    query_processors=[],
    stream_loader=KafkaStreamLoader(
        processor=SessionsProcessor(), default_topic="ingest-sessions",
    ),
)
# The materialized view we query aggregate data from.
materialized_storage = ReadableTableStorage(
    storage_key=StorageKey.SESSIONS_HOURLY,
    storage_set_key=StorageSetKey.SESSIONS,
    schemas=StorageSchemas(
        read_schema=read_schema,
        write_schema=None,
        intermediary_schemas=[materialized_view_schema],
    ),
    query_processors=[PrewhereProcessor()],
)
