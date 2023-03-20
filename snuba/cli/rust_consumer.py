import json
import os
from dataclasses import asdict, dataclass
from hashlib import md5
from typing import Optional, Sequence

import click

from snuba import settings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import (
    get_writable_storage,
    get_writable_storage_keys,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.settings.utils import get_settings_json

RUST_ENVIRONMENT = os.environ.get("RUST_ENVIRONMENT", "debug")
RUST_PATH = f"rust_snuba/target/{RUST_ENVIRONMENT}/consumer"


@click.command()
@click.option(
    "--storage",
    "storage_names",
    type=click.Choice(
        [storage_key.value for storage_key in get_writable_storage_keys()]
    ),
    help="The storage to target",
    multiple=True,
    required=True,
)
@click.option(
    "--consumer-group",
    help="Consumer group use for consuming the raw events topic.",
    required=True,
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option("--raw-events-topic", help="Topic to consume raw events from.")
@click.option(
    "--commit-log-topic",
    help="Topic for committed offsets to be written to, triggering post-processing task(s)",
)
@click.option(
    "--replacements-topic",
    help="Topic to produce replacement messages info.",
)
@click.option(
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    help="Kafka bootstrap server to use for consuming.",
)
@click.option(
    "--commit-log-bootstrap-server",
    "commit_log_bootstrap_servers",
    multiple=True,
    help="Kafka bootstrap server to use to produce the commit log.",
)
@click.option(
    "--replacement-bootstrap-server",
    "replacement_bootstrap_servers",
    multiple=True,
    help="Kafka bootstrap server to use to produce replacements.",
)
@click.option(
    "--slice-id",
    "slice_id",
    type=int,
    help="The slice id for the storage",
)
@click.option(
    "--log-level",
    "log_level",
    type=click.Choice(["error", "warn", "info", "debug", "trace"]),
    help="Logging level to use.",
    default="info",
)
def rust_consumer(
    *,
    storage_names: Sequence[str],
    consumer_group: str,
    auto_offset_reset: str,
    raw_events_topic: Optional[str],
    commit_log_topic: Optional[str],
    replacements_topic: Optional[str],
    bootstrap_servers: Sequence[str],
    commit_log_bootstrap_servers: Sequence[str],
    replacement_bootstrap_servers: Sequence[str],
    slice_id: Optional[int],
    log_level: str,
) -> None:
    """
    Experimental alternative to`snuba consumer`
    """

    settings_data = get_settings_json()
    settings_path = write_file_to_tmp_directory(settings_data, "settings.json")

    consumer_config = json.dumps(
        asdict(
            resolve_consumer_config(
                storage_names=storage_names,
                raw_topic=raw_events_topic,
                commit_log_topic=commit_log_topic,
                replacements_topic=replacements_topic,
                bootstrap_servers=bootstrap_servers,
                commit_log_bootstrap_servers=commit_log_bootstrap_servers,
                replacement_bootstrap_servers=replacement_bootstrap_servers,
                slice_id=slice_id,
            )
        )
    )

    config_hash = md5(consumer_config.encode("utf-8", "replace")).hexdigest()

    consumer_config_path = write_file_to_tmp_directory(
        consumer_config,
        f"consumer_config_{config_hash}.json",
    )

    rust_consumer_args = [
        "--",
        "--consumer-group",
        consumer_group,
        "--auto-offset-reset",
        auto_offset_reset,
        "--settings-path",
        settings_path,
        "--config-path",
        consumer_config_path,
    ]

    for storage_name in storage_names:
        rust_consumer_args.extend(["--storage", storage_name])

    os.execve(
        RUST_PATH,
        rust_consumer_args,
        {"RUST_LOG": log_level},
    )


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
class RustConsumerConfig:
    """
    Already resolved configuration for the Rust consumer
    """

    storages: Sequence[StorageConfig]
    raw_topic: str
    commit_log_topic: Optional[str]
    replacements_topic: Optional[str]


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
) -> RustConsumerConfig:
    """
    Resolves the ClickHouse cluster and Kafka brokers, and the physical topic name
    to be passed to the Rust consumer. We don't want to duplicate the cluster
    resolution code in Rust.
    """

    storages = {
        storage_name: get_writable_storage(StorageKey(storage_name))
        for storage_name in storage_names
    }

    validate_storages([*storages.values()])

    stream_loader = storages[storage_names[0]].get_table_writer().get_stream_loader()

    resolved_raw_topic: str
    if raw_topic is not None:
        resolved_raw_topic = raw_topic
    else:
        resolved_raw_topic = (
            stream_loader.get_default_topic_spec().get_physical_topic_name(slice_id)
        )

    resolved_commit_log_topic: Optional[str]
    commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()

    if commit_log_topic_spec is None:
        if commit_log_topic is not None:
            raise ValueError("Commit log not supported for this storage")
        resolved_commit_log_topic = None
    elif commit_log_topic is not None:
        resolved_commit_log_topic = commit_log_topic
    else:
        resolved_commit_log_topic = commit_log_topic_spec.get_physical_topic_name(
            slice_id
        )

    resolved_replacements_topic: Optional[str]
    replacements_topic_spec = stream_loader.get_replacement_topic_spec()

    if replacements_topic_spec is None:
        if replacements_topic is not None:
            raise ValueError("Commit log not supported for this storage")

        resolved_replacements_topic = None
    elif commit_log_topic is not None:
        resolved_commit_log_topic = replacements_topic
    else:
        resolved_commit_log_topic = replacements_topic_spec.get_physical_topic_name(
            slice_id
        )

    return RustConsumerConfig(
        storages=[
            resolve_storage_config(storage_name, storage)
            for (storage_name, storage) in storages.items()
        ],
        raw_topic=resolved_raw_topic,
        commit_log_topic=resolved_commit_log_topic,
        replacements_topic=resolved_replacements_topic,
    )


def resolve_storage_config(
    storage_name: str, storage: WritableTableStorage
) -> StorageConfig:
    # TODO: Temporarily hardcoded. To be properly resolved based on the storage set.
    hardcoded_clickhouse_cluster = ClickhouseClusterConfig(
        host="127.0.0.1", port=9000, user="default", password="", database="default"
    )

    processor = storage.get_table_writer().get_stream_loader().get_processor()

    table_schema = storage.get_schema()
    assert isinstance(table_schema, TableSchema)
    return StorageConfig(
        name=storage_name,
        clickhouse_table_name=table_schema.get_table_name(),
        clickhouse_cluster=hardcoded_clickhouse_cluster,
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


def write_file_to_tmp_directory(data: str, filename: str) -> str:
    """
    Write data to json file, return the file path.
    """

    tmp_dir = os.path.join(settings.ROOT_REPO_PATH, "tmp")
    file_path = os.path.join(tmp_dir, filename)

    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    with open(file_path, "w") as f:
        f.write(data)

    return file_path
