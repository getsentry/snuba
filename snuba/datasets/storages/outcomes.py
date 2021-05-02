from snuba.clickhouse.columns import UUID, ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.outcomes_processor import OutcomesProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.conditions_enforcer import OrgIdEnforcer
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.utils.streams.topics import Topic

WRITE_LOCAL_TABLE_NAME = "outcomes_raw_local"
WRITE_DIST_TABLE_NAME = "outcomes_raw_dist"
READ_LOCAL_TABLE_NAME = "outcomes_hourly_local"
READ_DIST_TABLE_NAME = "outcomes_hourly_dist"

write_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64, Modifiers(nullable=True))),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", String(Modifiers(nullable=True))),
        ("event_id", UUID(Modifiers(nullable=True))),
        ("quantity", UInt(32)),
        ("category", UInt(8)),
    ]
)

raw_schema = WritableTableSchema(
    columns=write_columns,
    # TODO: change to outcomes.raw_local when we add multi DB support
    local_table_name=WRITE_LOCAL_TABLE_NAME,
    dist_table_name=WRITE_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
)

read_columns = ColumnSet(
    [
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("key_id", UInt(64)),
        ("timestamp", DateTime()),
        ("outcome", UInt(8)),
        ("reason", String()),
        ("quantity", UInt(64)),
        ("category", UInt(8)),
        ("times_seen", UInt(64)),
    ]
)

read_schema = TableSchema(
    columns=read_columns,
    local_table_name=READ_LOCAL_TABLE_NAME,
    dist_table_name=READ_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.OUTCOMES,
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
        ("quantity", UInt(64)),
        ("category", UInt(8)),
    ]
)

materialized_view_schema = TableSchema(
    local_table_name="outcomes_mv_hourly_local",
    dist_table_name="outcomes_mv_hourly_dist",
    storage_set_key=StorageSetKey.OUTCOMES,
    columns=materialized_view_columns,
)

raw_storage = WritableTableStorage(
    storage_key=StorageKey.OUTCOMES_RAW,
    storage_set_key=StorageSetKey.OUTCOMES,
    schema=raw_schema,
    query_processors=[],
    mandatory_condition_checkers=[OrgIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        StorageKey.OUTCOMES_RAW,
        processor=OutcomesProcessor(),
        default_topic=Topic.OUTCOMES,
    ),
)

materialized_storage = ReadableTableStorage(
    storage_key=StorageKey.OUTCOMES_HOURLY,
    storage_set_key=StorageSetKey.OUTCOMES,
    schema=read_schema,
    query_processors=[PrewhereProcessor(["project_id", "org_id"])],
    mandatory_condition_checkers=[OrgIdEnforcer()],
)
