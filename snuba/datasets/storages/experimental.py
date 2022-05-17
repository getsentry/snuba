from snuba.clickhouse.columns import UUID, ColumnSet, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.profiles_processor import ProfilesMessageProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.utils.streams.topics import Topic

# THESE DO NOT EXIST!
EXPERIMENTAL_LOCAL_TABLE_NAME = "experimental_local"
EXPERIMENTAL_DIST_TABLE_NAME = "experimental_dist"


loader = build_kafka_stream_loader_from_settings(
    # this is broken (it is supposed to be)
    processor=ProfilesMessageProcessor(),
    default_topic=Topic.EXPERIMENTAL,
)

readable_columns = ColumnSet(
    [
        ("organization_id", UInt(64)),
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),
    ]
)

writable_columns = readable_columns + ColumnSet(
    [("retention_days", UInt(16)), ("partition", UInt(16)), ("offset", UInt(64))]
)

writable_schema = WritableTableSchema(
    columns=writable_columns,
    local_table_name=EXPERIMENTAL_DIST_TABLE_NAME,
    dist_table_name=EXPERIMENTAL_DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.PROFILES,
)

writable_storage = WritableTableStorage(
    storage_key=StorageKey.EXPERIMENTAL,
    storage_set_key=StorageSetKey.EXPERIMENTAL,
    schema=writable_schema,
    query_processors=[],
    mandatory_condition_checkers=[],
    stream_loader=loader,
)
