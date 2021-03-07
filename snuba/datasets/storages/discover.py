from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    IPv4,
    IPv6,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors_common import mandatory_conditions
from snuba.datasets.storages.event_id_column_processor import EventIdColumnProcessor
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.uuid_column_processor import UUIDColumnProcessor
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy

columns = ColumnSet(
    [
        ("event_id", UUID()),
        ("project_id", UInt(64)),
        ("type", String()),
        ("timestamp", DateTime()),
        ("platform", String()),
        ("environment", String(Modifiers(nullable=True))),
        ("release", String(Modifiers(nullable=True))),
        ("dist", String(Modifiers(nullable=True))),
        ("transaction_name", String()),
        ("message", String()),
        ("title", String()),
        ("user", String()),
        ("user_hash", UInt(64)),
        ("user_id", String(Modifiers(nullable=True))),
        ("user_name", String(Modifiers(nullable=True))),
        ("user_email", String(Modifiers(nullable=True))),
        ("ip_address_v4", IPv4(Modifiers(nullable=True))),
        ("ip_address_v6", IPv6(Modifiers(nullable=True))),
        ("sdk_name", String(Modifiers(nullable=True))),
        ("sdk_version", String(Modifiers(nullable=True))),
        ("http_method", String(Modifiers(nullable=True))),
        ("http_referer", String(Modifiers(nullable=True))),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Array(UInt(64))),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("deleted", UInt(8)),
    ]
)

schema = TableSchema(
    columns=columns,
    local_table_name="discover_local",
    dist_table_name="discover_dist",
    storage_set_key=StorageSetKey.DISCOVER,
    mandatory_conditions=mandatory_conditions,
)

storage = ReadableTableStorage(
    storage_key=StorageKey.DISCOVER,
    storage_set_key=StorageSetKey.DISCOVER,
    schema=schema,
    query_processors=[
        MappingOptimizer("tags", "_tags_hash_map", "tags_hash_map_enabled"),
        EventIdColumnProcessor(),
        ArrayJoinKeyValueOptimizer("tags"),
        UUIDColumnProcessor(set(["event_id"])),
        PrewhereProcessor(
            [
                "event_id",
                "release",
                "message",
                "transaction_name",
                "environment",
                "project_id",
            ]
        ),
    ],
    query_splitters=[
        ColumnSplitQueryStrategy(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="timestamp",
        ),
        TimeSplitQueryStrategy(timestamp_col="timestamp"),
    ],
)
