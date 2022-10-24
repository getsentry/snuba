from snuba.clickhouse.columns import UUID, ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.processors.profiles_processor import ProfilesMessageProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.condition_checkers.checkers import (
    OrgIdEnforcer,
    ProjectIdEnforcer,
)
from snuba.query.processors.physical.table_rate_limit import TableRateLimit
from snuba.query.processors.physical.uuid_column_processor import UUIDColumnProcessor
from snuba.utils.streams.topics import Topic

PROFILES_LOCAL_TABLE_NAME = "profiles_local"
PROFILES_DIST_TABLE_NAME = "profiles_dist"


processors = [
    UUIDColumnProcessor(set(["profile_id", "transaction_id", "trace_id"])),
    TableRateLimit(),
]

loader = build_kafka_stream_loader_from_settings(
    processor=ProfilesMessageProcessor(),
    default_topic=Topic.PROFILES,
)

readable_columns = ColumnSet(
    [
        ("organization_id", UInt(64)),
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),
        ("profile_id", UUID()),
        ("received", DateTime()),
        ("android_api_level", UInt(32, Modifiers(nullable=True))),
        ("device_classification", String()),
        ("device_locale", String()),
        ("device_manufacturer", String()),
        ("device_model", String()),
        ("device_os_build_number", String(Modifiers(nullable=True))),
        ("device_os_name", String()),
        ("device_os_version", String()),
        ("architecture", String()),
        ("duration_ns", UInt(64)),
        ("environment", String(Modifiers(nullable=True))),
        ("platform", String()),
        ("trace_id", UUID()),
        ("transaction_name", String()),
        ("version_name", String()),
        ("version_code", String()),
    ]
)

writable_columns = readable_columns + ColumnSet(
    [("retention_days", UInt(16)), ("partition", UInt(16)), ("offset", UInt(64))]
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
    mandatory_condition_checkers=[
        OrgIdEnforcer("organization_id"),
        ProjectIdEnforcer(),
    ],
    stream_loader=loader,
)
