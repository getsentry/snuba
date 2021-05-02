from datetime import timedelta

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    ColumnSet,
    DateTime,
    String,
    UInt,
)
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.sessions_processor import SessionsProcessor
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.exceptions import ValidationException
from snuba.query.processors.conditions_enforcer import OrgIdEnforcer, ProjectIdEnforcer
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.request.request_settings import RequestSettings
from snuba.utils.streams.topics import Topic

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
        ("quantity", UInt(32)),
        ("seq", UInt(64)),
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("retention_days", UInt(16)),
        ("duration", UInt(32)),
        ("status", UInt(8)),
        ("errors", UInt(16)),
        ("received", DateTime()),
        ("started", DateTime()),
        ("release", String()),
        ("environment", String()),
        ("user_agent", String()),
        ("os", String()),
    ]
)

raw_schema = WritableTableSchema(
    columns=all_columns,
    local_table_name=WRITE_LOCAL_TABLE_NAME,
    dist_table_name=WRITE_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
)

read_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("started", DateTime()),
        ("release", String()),
        ("environment", String()),
        ("user_agent", String()),
        ("os", String()),
        (
            "duration_quantiles",
            AggregateFunction("quantilesIf(0.5, 0.9)", [UInt(32), UInt(8)]),
        ),
        ("duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])),
        ("sessions", AggregateFunction("countIf", [UUID(), UInt(8)])),
        ("sessions_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
        ("sessions_crashed", AggregateFunction("countIf", [UUID(), UInt(8)])),
        ("sessions_crashed_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
        ("sessions_abnormal", AggregateFunction("countIf", [UUID(), UInt(8)])),
        ("sessions_abnormal_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
        ("sessions_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
        ("sessions_errored_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
        ("users", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
        ("users_crashed", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
        ("users_abnormal", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
        ("users_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    ]
)
read_schema = TableSchema(
    columns=read_columns,
    local_table_name=READ_LOCAL_TABLE_NAME,
    dist_table_name=READ_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
)
materialized_view_schema = TableSchema(
    local_table_name=READ_LOCAL_MV_NAME,
    dist_table_name=READ_DIST_MV_NAME,
    storage_set_key=StorageSetKey.SESSIONS,
    columns=read_columns,
)


class MinuteResolutionProcessor(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # NOTE: the product side is restricted to a 6h window, however it rounds
        # outwards, which extends the window to 7h.
        from_date, to_date = get_time_range(query, "started")
        if not from_date or not to_date or (to_date - from_date) > timedelta(hours=7):
            raise ValidationException(
                "Minute-resolution queries are restricted to a 7-hour time window."
            )


# The raw table we write onto, and that potentially we could
# query.
raw_storage = WritableTableStorage(
    storage_key=StorageKey.SESSIONS_RAW,
    storage_set_key=StorageSetKey.SESSIONS,
    schema=raw_schema,
    query_processors=[MinuteResolutionProcessor()],
    mandatory_condition_checkers=[OrgIdEnforcer(), ProjectIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        StorageKey.SESSIONS_RAW,
        processor=SessionsProcessor(),
        default_topic=Topic.SESSIONS,
    ),
)
# The materialized view we query aggregate data from.
materialized_storage = ReadableTableStorage(
    storage_key=StorageKey.SESSIONS_HOURLY,
    storage_set_key=StorageSetKey.SESSIONS,
    schema=read_schema,
    query_processors=[PrewhereProcessor(["project_id", "org_id"])],
    mandatory_condition_checkers=[OrgIdEnforcer(), ProjectIdEnforcer()],
)
