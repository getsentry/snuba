from arroyo import Topic as KafkaTopic
from arroyo.backends.kafka import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)

from snuba.clickhouse.columns import UUID, Array, ColumnSet, DateTime, IPv4, IPv6
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.processors.replays_processor import ReplaysProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.condition_checkers.checkers import ProjectIdEnforcer
from snuba.query.processors.physical.table_rate_limit import TableRateLimit
from snuba.utils.schemas import Nested
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic

LOCAL_TABLE_NAME = "replays_local"
DIST_TABLE_NAME = "replays_dist"

columns = ColumnSet(
    [
        ("replay_id", UUID()),
        ("replay_type", String(Modifiers(nullable=True))),
        ("event_hash", UUID()),
        ("segment_id", UInt(16, Modifiers(nullable=True))),
        ("timestamp", DateTime()),
        ("replay_start_timestamp", DateTime(Modifiers(nullable=True))),
        (
            "trace_ids",
            Array(UUID()),
        ),
        (
            "error_ids",
            Array(UUID()),
        ),
        ("title", String(Modifiers(readonly=True))),
        ("url", String(Modifiers(nullable=True))),
        ("urls", Array(String())),
        ("is_archived", UInt(8, Modifiers(nullable=True))),
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
        ("user_id", String(Modifiers(nullable=True))),
        ("user_name", String(Modifiers(nullable=True))),
        ("user_email", String(Modifiers(nullable=True))),
        # OS
        ("os_name", String(Modifiers(nullable=True))),
        ("os_version", String(Modifiers(nullable=True))),
        # Browser
        ("browser_name", String(Modifiers(nullable=True))),
        ("browser_version", String(Modifiers(nullable=True))),
        # Device
        ("device_name", String(Modifiers(nullable=True))),
        ("device_brand", String(Modifiers(nullable=True))),
        ("device_family", String(Modifiers(nullable=True))),
        ("device_model", String(Modifiers(nullable=True))),
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


def produce_policy_creator() -> DeadLetterQueuePolicy:
    """Produce all bad messages to dead-letter topic."""
    return ProduceInvalidMessagePolicy(
        KafkaProducer(build_kafka_producer_configuration(Topic.DEAD_LETTER_REPLAYS)),
        KafkaTopic(Topic.DEAD_LETTER_REPLAYS.value),
    )


storage = WritableTableStorage(
    storage_key=StorageKey.REPLAYS,
    storage_set_key=StorageSetKey.REPLAYS,
    schema=schema,
    query_processors=[TableRateLimit()],
    mandatory_condition_checkers=[ProjectIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=ReplaysProcessor(),
        default_topic=Topic.REPLAYEVENTS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
)
