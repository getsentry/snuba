from snuba.clickhouse.columns import UUID, ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.functions_processor import FunctionsMessageProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.processors.type_converters.uuid_column_processor import (
    UUIDColumnProcessor,
)
from snuba.utils.streams.topics import Topic

columns = ColumnSet(
    [
        # profile level columns - denormalized
        ("org_id", UInt(64)),
        ("project_id", UInt(64)),
        ("profile_id", UUID()),
        ("transaction_id", UUID()),
        ("trace_id", UUID()),
        ("received", DateTime()),
        ("android_api_level", UInt(32)),
        ("device_classification", String()),
        ("device_locale", String()),
        ("device_manufacturer", String()),
        ("device_model", String()),
        ("device_os_build_number", String()),
        ("device_os_name", String()),
        ("device_os_version", String()),
        ("duration_ns", UInt(64)),
        ("environment", String(Modifiers(nullable=True))),
        ("platform", String()),
        ("transaction_name", String()),
        ("version_name", String()),
        ("version_code", String()),
        # function level columns
        ("function_id", UInt(64)),
        ("symbol", String()),
        ("address", UInt(64)),
        ("thread_id", UInt(64)),
        ("filename", String()),
        ("line", UInt(32)),
        ("self_time", UInt(64)),
        ("duration", UInt(64)),
        ("timestamp", UInt(64)),
        ("fingerprint", UInt(64)),
        ("parent_fingerprint", UInt(64)),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="functions_local",
    dist_table_name="functions_dist",
    storage_set_key=StorageSetKey.PROFILES,
)

storage = WritableTableStorage(
    storage_key=StorageKey.FUNCTIONS,
    storage_set_key=StorageSetKey.PROFILES,
    schema=schema,
    query_processors=[
        UUIDColumnProcessor(set(["profile_id", "transaction_id", "trace_id"])),
        TableRateLimit(),
    ],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=FunctionsMessageProcessor(),
        default_topic=Topic.PROFILES,
    ),
)
