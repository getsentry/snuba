import json
from dataclasses import asdict
from typing import Optional, Sequence

import click

from snuba import settings, state
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.datasets.storages.factory import get_writable_storage_keys


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
@click.option(
    "--no-strict-offset-reset",
    is_flag=True,
    help="Forces the kafka consumer auto offset reset.",
)
@click.option(
    "--queued-max-messages-kbytes",
    default=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    type=int,
    help="Maximum number of kilobytes per topic+partition in the local consumer queue.",
)
@click.option(
    "--queued-min-messages",
    default=settings.DEFAULT_QUEUED_MIN_MESSAGES,
    type=int,
    help="Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.",
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
    "--max-batch-size",
    default=settings.DEFAULT_MAX_BATCH_SIZE,
    type=int,
    help="Max number of messages to batch in memory before writing to Kafka.",
)
@click.option(
    "--max-batch-time-ms",
    default=settings.DEFAULT_MAX_BATCH_TIME_MS,
    type=int,
    help="Max length of time to buffer messages in memory before writing to Kafka.",
)
@click.option(
    "--log-level",
    "log_level",
    type=click.Choice(["error", "warn", "info", "debug", "trace"], False),
    help="Logging level to use.",
    default="info",
)
@click.option(
    "--skip-write/--no-skip-write",
    "skip_write",
    help="Skip the write to clickhouse",
    default=True,
)
@click.option(
    "--concurrency",
    type=int,
)
@click.option(
    "--use-rust-processor/--use-python-processor",
    "use_rust_processor",
    is_flag=True,
    help="Use the Rust (if available) or Python message processor",
    default=False,
)
@click.option(
    "--group-instance-id",
    type=str,
    default=None,
    help="Kafka group instance id. passing a value here will run kafka with static membership.",
)
@click.option(
    "--python-max-queue-depth",
    type=int,
    default=None,
    help="How many messages should be queued up in the Python message processor before backpressure kicks in. Defaults to the number of processes.",
)
@click.option(
    "--max-poll-interval-ms",
    type=int,
    default=30000,
)
@click.option(
    "--health-check-file",
    default=None,
    type=str,
    help="Arroyo will touch this file at intervals to indicate health. If not provided, no health check is performed.",
)
def rust_consumer(
    *,
    storage_names: Sequence[str],
    consumer_group: str,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    raw_events_topic: Optional[str],
    commit_log_topic: Optional[str],
    replacements_topic: Optional[str],
    bootstrap_servers: Sequence[str],
    commit_log_bootstrap_servers: Sequence[str],
    replacement_bootstrap_servers: Sequence[str],
    slice_id: Optional[int],
    max_batch_size: int,
    max_batch_time_ms: int,
    log_level: str,
    skip_write: bool,
    concurrency: Optional[int],
    use_rust_processor: bool,
    group_instance_id: Optional[str],
    max_poll_interval_ms: int,
    python_max_queue_depth: Optional[int],
    health_check_file: Optional[str],
) -> None:
    """
    Experimental alternative to `snuba consumer`
    """

    consumer_config = resolve_consumer_config(
        storage_names=storage_names,
        raw_topic=raw_events_topic,
        commit_log_topic=commit_log_topic,
        replacements_topic=replacements_topic,
        bootstrap_servers=bootstrap_servers,
        commit_log_bootstrap_servers=commit_log_bootstrap_servers,
        replacement_bootstrap_servers=replacement_bootstrap_servers,
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
        slice_id=slice_id,
        group_instance_id=group_instance_id,
    )

    consumer_config_raw = json.dumps(asdict(consumer_config))

    import os

    import rust_snuba

    # TODO: remove after debugging
    os.environ["RUST_LOG"] = "debug" if not use_rust_processor else log_level.lower()

    # XXX: Temporary way to quickly test different values for concurrency
    # Should be removed before this is put into  prod
    concurrency_override = state.get_int_config(
        f"rust_consumer.{storage_names[0]}.concurrency"
    )

    rust_snuba.consumer(  # type: ignore
        consumer_group,
        auto_offset_reset,
        no_strict_offset_reset,
        consumer_config_raw,
        skip_write,
        concurrency_override or concurrency or 1,
        use_rust_processor,
        max_poll_interval_ms,
        python_max_queue_depth,
        health_check_file,
    )
