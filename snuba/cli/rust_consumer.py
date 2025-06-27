import json
import sys
from dataclasses import asdict
from typing import Optional, Sequence

import click

from snuba import settings
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
    default="earliest",
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
    "--concurrency",
    type=int,
)
@click.option(
    "--clickhouse-concurrency",
    type=int,
    help="Number of concurrent clickhouse batches at one time.",
)
@click.option(
    "--use-rust-processor/--use-python-processor",
    "use_rust_processor",
    is_flag=True,
    help="Use the Rust (if available) or Python message processor",
    default=True,
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
    "--async-inserts",
    is_flag=True,
    default=False,
    help="Enable async inserts for ClickHouse",
)
@click.option(
    "--max-dlq-buffer-length",
    type=int,
    default=None,
    help="Set a per-partition limit to the length of the DLQ buffer",
)
@click.option(
    "--health-check-file",
    default=None,
    type=str,
    help="Arroyo will touch this file at intervals to indicate health. If not provided, no health check is performed.",
)
@click.option(
    "--enforce-schema",
    type=bool,
    is_flag=True,
    default=False,
    help="Enforce schema on the raw events topic.",
)
@click.option(
    "--stop-at-timestamp",
    type=int,
    help="Unix timestamp after which to stop processing messages",
)
@click.option(
    "--batch-write-timeout-ms",
    type=int,
    default=None,
    help="Optional timeout for batch writer client connecting and sending request to Clickhouse",
)
@click.option(
    "--custom-envoy-request-timeout",
    type=int,
    default=None,
    help="Optional request timeout value for Snuba -> Envoy -> Clickhouse connection",
)
@click.option(
    "--quantized-rebalance-consumer-group-delay-secs",
    type=int,
    default=None,
    help="Quantized rebalancing means that during deploys, rebalancing is triggered across all pods within a consumer group at the same time. The value is used by the pods to align their group join/leave activity to some multiple of the delay",
)
@click.option(
    "--join-timeout-ms",
    type=int,
    default=1000,
    help="number of milliseconds to wait for the current batch to be flushed by the consumer in case of rebalance",
)
@click.option(
    "--health-check",
    default="arroyo",
    type=click.Choice(["snuba", "arroyo"]),
    help="Specify which health check to use for the consumer. If not specified, the default Arroyo health check is used.",
)
@click.option(
    "--consumer-version",
    default="v2",
    type=click.Choice(["v1", "v2"]),
    help="Specify which consumer version to use, v1 is stable, v2 is experimental",
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
    concurrency: Optional[int],
    clickhouse_concurrency: Optional[int],
    use_rust_processor: bool,
    group_instance_id: Optional[str],
    max_poll_interval_ms: int,
    async_inserts: bool,
    health_check: str,
    python_max_queue_depth: Optional[int],
    health_check_file: Optional[str],
    enforce_schema: bool,
    stop_at_timestamp: Optional[int],
    batch_write_timeout_ms: Optional[int],
    max_dlq_buffer_length: Optional[int],
    quantized_rebalance_consumer_group_delay_secs: Optional[int],
    custom_envoy_request_timeout: Optional[int],
    join_timeout_ms: Optional[int],
    consumer_version: Optional[str],
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
        quantized_rebalance_consumer_group_delay_secs=quantized_rebalance_consumer_group_delay_secs,
        custom_envoy_request_timeout=custom_envoy_request_timeout,
    )

    consumer_config_raw = json.dumps(asdict(consumer_config))

    import os

    import rust_snuba

    os.environ["RUST_LOG"] = log_level.lower()

    if not async_inserts:
        # we don't want to allow increasing this if
        # we aren't using async inserts since that will increase
        # the number of inserts/sec on clickhouse
        clickhouse_concurrency = 2

    exitcode = rust_snuba.consumer(  # type: ignore
        consumer_group,
        auto_offset_reset,
        no_strict_offset_reset,
        consumer_config_raw,
        concurrency or 1,
        clickhouse_concurrency or 2,
        use_rust_processor,
        enforce_schema,
        max_poll_interval_ms,
        async_inserts,
        health_check,
        python_max_queue_depth,
        health_check_file,
        stop_at_timestamp,
        batch_write_timeout_ms,
        max_dlq_buffer_length,
        custom_envoy_request_timeout,
        join_timeout_ms,
        consumer_version,
    )

    sys.exit(exitcode)
