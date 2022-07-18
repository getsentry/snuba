from snuba.clickhouse.columns import UUID, Array, ColumnSet, DateTime, IPv4, IPv6
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.replays_processor import ReplaysProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.conditions_enforcer import ProjectIdEnforcer
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.utils.schemas import Nested
from snuba.utils.streams.topics import Topic

LOCAL_TABLE_NAME = "replays_local"
DIST_TABLE_NAME = "replays_dist"

columns = ColumnSet(
    [
        ("replay_id", UUID()),
        ("sequence_id", UInt(16)),
        ("timestamp", DateTime()),
        (
            "trace_ids",
            Array(UUID()),
        ),  # TODO: create bloom filter index / materialize column
        ("title", String(Modifiers(readonly=True))),
        ### common sentry event columns
        ("project_id", UInt(64)),
        # release/environment info
        ("platform", String()),
        ("environment", String(Modifiers(nullable=True))),
        ("release", String(Modifiers(nullable=True))),
        ("dist", String(Modifiers(nullable=True))),
        ("ip_address_v4", IPv4(Modifiers(nullable=True))),
        ("ip_address_v6", IPv6(Modifiers(nullable=True))),
        # user columns
        ("user", String()),
        ("user_hash", UInt(64, Modifiers(readonly=True))),
        ("user_id", String(Modifiers(nullable=True))),
        ("user_name", String(Modifiers(nullable=True))),
        ("user_email", String(Modifiers(nullable=True))),
        # sdk info
        ("sdk_name", String()),
        ("sdk_version", String()),
        ("tags", Nested([("key", String()), ("value", String())])),
        # deletion info
        ("retention_days", UInt(16)),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name=LOCAL_TABLE_NAME,
    dist_table_name=DIST_TABLE_NAME,
    storage_set_key=StorageSetKey.REPLAYS,
)

# TODO: set up deadletter queue for bad messages.

storage = WritableTableStorage(
    storage_key=StorageKey.REPLAYS,
    storage_set_key=StorageSetKey.REPLAYS,
    schema=schema,
    query_processors=[TableRateLimit()],
    mandatory_condition_checkers=[ProjectIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=ReplaysProcessor(),
        default_topic=Topic.REPLAYEVENTS,
    ),
)
