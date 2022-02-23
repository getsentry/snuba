from snuba.clickhouse.columns import UUID, ColumnSet, DateTime, NamedTuple, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.profiles_processor import ProfilesProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.conditions_enforcer import OrgIdEnforcer, ProjectIdEnforcer
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.processors.type_converters.uniq_in_select_and_having import (
    UniqInSelectAndHavingProcessor,
)
from snuba.query.processors.type_converters.uuid_column_processor import (
    UUIDColumnProcessor,
)
from snuba.utils.streams.topics import Topic

PROFILES_LOCAL_TABLE_NAME = "profiles_local"
PROFILES_DIST_TABLE_NAME = "profiles_dist"


writable_columns = ColumnSet(
    [
        ("organization_id", UInt(64)),
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),
        ("received", DateTime()),
        ("profile", String()),
        ("symbols", String()),
        ("android_api_level", UInt(32)),
        ("device_classification", String()),
        ("device_locale", String()),
        ("device_manufacturer", String()),
        ("device_model", String()),
        ("device_os_build_number", String()),
        ("device_os_name", String()),
        ("device_os_version", String()),
        ("duration_ns", UInt(64)),
        ("environment", String()),
        ("error_code", String()),
        ("error_description", String()),
        ("platform", String()),
        ("trace_id", UUID()),
        ("transaction_name", String()),
        ("version", NamedTuple((("name", String()), ("code", String())))),
        ("retention_days", UInt(16)),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
    ]
)

processors = [
    UniqInSelectAndHavingProcessor(),
    UUIDColumnProcessor(set(["transaction_id"])),
    TableRateLimit(),
]

loader = build_kafka_stream_loader_from_settings(
    processor=ProfilesProcessor(), default_topic=Topic.PROFILES,
)

writable_schema = WritableTableSchema(
    columns=writable_columns,
    local_table_name=PROFILES_LOCAL_TABLE_NAME,
    dist_table_name=PROFILES_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.PROFILES,
)

writable_storage = WritableTableStorage(
    storage_key=StorageKey.PROFILES,
    storage_set_key=StorageSetKey.PROFILES,
    schema=writable_schema,
    query_processors=processors,
    mandatory_condition_checkers=[OrgIdEnforcer(), ProjectIdEnforcer()],
    stream_loader=loader,
)

readable_columns = ColumnSet(
    [
        ("organization_id", UInt(64)),
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),
        ("received", DateTime()),
        ("profile", String()),
        ("symbols", String()),
        ("android_api_level", UInt(32,)),
        ("device_classification", String()),
        ("device_locale", String()),
        ("device_manufacturer", String()),
        ("device_model", String()),
        ("device_os_build_number", String()),
        ("device_os_name", String()),
        ("device_os_version", String()),
        ("duration_ns", UInt(64)),
        ("environment", String()),
        ("error_code", String()),
        ("error_description", String()),
        ("platform", String()),
        ("trace_id", UUID()),
        ("transaction_name", String()),
        ("version", NamedTuple((("name", String()), ("code", String())))),
    ]
)

readable_schema = TableSchema(
    columns=readable_columns,
    local_table_name=PROFILES_LOCAL_TABLE_NAME,
    dist_table_name=PROFILES_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.PROFILES,
)

readable_storage = ReadableTableStorage(
    storage_key=StorageKey.PROFILES,
    storage_set_key=StorageSetKey.PROFILES,
    schema=readable_schema,
    query_processors=processors,
)
