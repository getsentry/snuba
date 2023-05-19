from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from arroyo.backends.kafka import build_kafka_configuration

from snuba import settings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.utils.streams.configuration_builder import _get_default_topic_configuration


@dataclass(frozen=True)
class ClickhouseClusterConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class MessageProcessorConfig:
    python_class_name: str
    python_module: str


@dataclass(frozen=True)
class StorageConfig:
    name: str
    clickhouse_table_name: str
    clickhouse_cluster: ClickhouseClusterConfig
    message_processor: MessageProcessorConfig


@dataclass(frozen=True)
class TopicConfig:
    broker_config: Mapping[str, Any]
    physical_topic_name: str


@dataclass(frozen=True)
class EnvConfig:
    sentry_dsn: Optional[str]


@dataclass(frozen=True)
class ConsumerConfig:
    """
    Resolved configuration shared by the Python and Rust consumers
    """

    storages: Sequence[StorageConfig]
    raw_topic: TopicConfig
    commit_log_topic: Optional[TopicConfig]
    replacements_topic: Optional[TopicConfig]
    max_batch_size: int
    max_batch_time_ms: int
    env: Optional[EnvConfig]


def _resolve_topic_config(
    param: str,
    topic_spec: Optional[KafkaTopicSpec],
    cli_param: Optional[str],
    slice_id: Optional[int],
) -> Optional[TopicConfig]:
    if topic_spec is None:
        if cli_param is not None:
            raise ValueError(f"{param} not supported for this storage")
        return None
    elif cli_param is not None:
        physical_topic_name = cli_param
    else:
        physical_topic_name = topic_spec.get_physical_topic_name(slice_id)

    broker = build_kafka_configuration(
        _get_default_topic_configuration(topic_spec.topic, slice_id)
    )
    return TopicConfig(broker_config=broker, physical_topic_name=physical_topic_name)


def _resolve_env_config() -> Optional[EnvConfig]:
    sentry_dsn = settings.SENTRY_DSN
    return EnvConfig(sentry_dsn=sentry_dsn)


def resolve_consumer_config(
    *,
    storage_names: Sequence[str],
    raw_topic: Optional[str],
    commit_log_topic: Optional[str],
    replacements_topic: Optional[str],
    bootstrap_servers: Sequence[str],
    commit_log_bootstrap_servers: Sequence[str],
    replacement_bootstrap_servers: Sequence[str],
    slice_id: Optional[int],
    max_batch_size: int,
    max_batch_time_ms: int,
) -> ConsumerConfig:
    """
    Resolves the ClickHouse cluster and Kafka brokers, and the physical topic name
    to be passed to the consumer. This avoids duplicating the cluster resolution code
    in Python and Rust.
    """

    storages = {
        storage_name: get_writable_storage(StorageKey(storage_name))
        for storage_name in storage_names
    }

    validate_storages([*storages.values()])

    stream_loader = storages[storage_names[0]].get_table_writer().get_stream_loader()
    default_topic_spec = stream_loader.get_default_topic_spec()

    resolved_raw_topic = _resolve_topic_config(
        "main topic", default_topic_spec, raw_topic, slice_id
    )
    assert resolved_raw_topic is not None

    commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
    resolved_commit_log_topic = _resolve_topic_config(
        "commit log", commit_log_topic_spec, commit_log_topic, slice_id
    )

    replacements_topic_spec = stream_loader.get_replacement_topic_spec()
    resolved_replacements_topic = _resolve_topic_config(
        "replacements topic", replacements_topic_spec, replacements_topic, slice_id
    )

    resolved_env_config = _resolve_env_config()

    return ConsumerConfig(
        storages=[
            resolve_storage_config(storage_name, storage)
            for (storage_name, storage) in storages.items()
        ],
        raw_topic=resolved_raw_topic,
        commit_log_topic=resolved_commit_log_topic,
        replacements_topic=resolved_replacements_topic,
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        env=resolved_env_config,
    )


def resolve_storage_config(
    storage_name: str, storage: WritableTableStorage
) -> StorageConfig:
    cluster = storage.get_cluster()
    user, password = cluster.get_credentials()
    clickhouse_cluster = ClickhouseClusterConfig(
        host=cluster.get_host(),
        port=cluster.get_port(),
        user=user,
        password=password,
        database=cluster.get_database(),
    )

    processor = storage.get_table_writer().get_stream_loader().get_processor()

    table_schema = storage.get_schema()
    assert isinstance(table_schema, TableSchema)
    return StorageConfig(
        name=storage_name,
        clickhouse_table_name=table_schema.get_table_name(),
        clickhouse_cluster=clickhouse_cluster,
        message_processor=MessageProcessorConfig(
            python_class_name=processor.__class__.__name__,
            python_module=processor.__class__.__module__,
        ),
    )


def validate_storages(storages: Sequence[WritableTableStorage]) -> None:
    """
    Validates that storage combination is valid based on topic definitions
    """
    assert (
        len(
            set(
                [
                    storage.get_table_writer()
                    .get_stream_loader()
                    .get_default_topic_spec()
                    for storage in storages
                ]
            )
        )
        < 2
    ), "All storages must have the same default topic spec"

    assert (
        len(
            set(
                [
                    storage.get_table_writer()
                    .get_stream_loader()
                    .get_commit_log_topic_spec()
                    for storage in storages
                ]
            )
        )
        < 2
    ), "All storages must have the same commit log topic spec"

    assert (
        len(
            set(
                [
                    storage.get_table_writer()
                    .get_stream_loader()
                    .get_replacement_topic_spec()
                    for storage in storages
                ]
            )
        )
        < 2
    ), "All storages must have the same replacement topic spec"
