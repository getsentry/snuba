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
    "storage_name",
    type=click.Choice([storage_key.value for storage_key in get_writable_storage_keys()]),
    help="The storage to target",
    required=True,
)
@click.option(
    "--consumer-group",
    help="Consumer group use for consuming the raw topic.",
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
@click.option("--raw-topic", help="Topic to consumer from.")
@click.option("--produce-topic", help="Topic to produce outcomes to.")
@click.option(
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    help="Kafka bootstrap server to use for consuming.",
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
    "--bucket-interval",
    default=60,
    type=int,
    help="Bucket interval in seconds for accepted outcomes aggregation.",
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
    "--max-poll-interval-ms",
    type=int,
    default=30000,
)
@click.option(
    "--max-dlq-buffer-length",
    type=int,
    default=25000,
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
def accepted_outcomes_consumer(
    *,
    storage_name: str,
    consumer_group: str,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    raw_topic: Optional[str],
    produce_topic: str,
    bootstrap_servers: Sequence[str],
    max_batch_size: int,
    max_batch_time_ms: int,
    bucket_interval: int,
    log_level: str,
    concurrency: Optional[int],
    max_poll_interval_ms: int,
    health_check: str,
    health_check_file: Optional[str],
    enforce_schema: bool,
    max_dlq_buffer_length: Optional[int],
    join_timeout_ms: Optional[int],
) -> None:
    """
    Accepted outcomes consumer
    """

    consumer_config = resolve_consumer_config(
        storage_names=[storage_name],
        raw_topic=raw_topic,
        commit_log_topic=None,
        replacements_topic=None,
        bootstrap_servers=bootstrap_servers,
        commit_log_bootstrap_servers=[],
        replacement_bootstrap_servers=[],
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
        slice_id=None,
        group_instance_id=None,
        quantized_rebalance_consumer_group_delay_secs=None,
    )

    consumer_config_raw = json.dumps(asdict(consumer_config))

    import os

    import rust_snuba

    os.environ["RUST_LOG"] = log_level.lower()
    produce_topic = produce_topic

    exitcode = rust_snuba.accepted_outcomes_consumer(  # type: ignore
        consumer_group,
        auto_offset_reset,
        no_strict_offset_reset,
        consumer_config_raw,
        concurrency or 1,
        enforce_schema,
        max_poll_interval_ms,
        health_check,
        health_check_file,
        max_dlq_buffer_length,
        join_timeout_ms,
        max_batch_time_ms,
        bucket_interval,
        produce_topic,
    )

    sys.exit(exitcode)
