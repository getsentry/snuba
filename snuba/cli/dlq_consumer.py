import logging
import signal
import time
from dataclasses import replace
from typing import Any, Optional

import click
from arroyo import configure_metrics
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing import StreamProcessor

from snuba import environment, settings
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.consumers.dlq import (
    DlqInstructionStatus,
    clear_instruction,
    load_instruction,
    mark_instruction_in_progress,
)
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--consumer-group",
    help="Consumer group use for consuming the raw events topic.",
    required=True,
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
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--processes",
    type=int,
)
@click.option(
    "--input-block-size",
    type=int,
)
@click.option(
    "--output-block-size",
    type=int,
)
def dlq_consumer(
    *,
    consumer_group: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
    log_level: Optional[str] = None,
) -> None:
    shutdown_requested = False
    consumer: Optional[StreamProcessor[KafkaPayload]] = None

    def handler(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

        if consumer is not None:
            consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    while not shutdown_requested:
        instruction = load_instruction()

        if (
            instruction is None
            or instruction.status != DlqInstructionStatus.NOT_STARTED
        ):
            time.sleep(2.0)
            continue

        mark_instruction_in_progress()

        logger.info(f"Starting DLQ consumer {instruction}")

        metrics_tags = {
            "consumer_group": consumer_group,
            "storage": instruction.storage_key.value,
            "slice_id": str(instruction.slice_id),
            "policy": instruction.policy.value,
        }

        metrics = MetricsWrapper(environment.metrics, "dlq_consumer", tags=metrics_tags)

        configure_metrics(StreamMetricsAdapter(metrics), force=True)

        orig_consumer_config = resolve_consumer_config(
            storage_names=[instruction.storage_key.value],
            # Topics and bootstrap servers cannot be overridden in the DLQ consumer
            raw_topic=None,
            commit_log_topic=None,
            replacements_topic=None,
            bootstrap_servers=[],
            commit_log_bootstrap_servers=[],
            replacement_bootstrap_servers=[],
            slice_id=instruction.slice_id,
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
        )
        # DLQ consumer should never write to the commit log as those entries should be ordered
        consumer_config = replace(orig_consumer_config, commit_log_topic=None)

        consumer_builder = ConsumerBuilder(
            consumer_config=consumer_config,
            kafka_params=KafkaParameters(
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                strict_offset_reset=not no_strict_offset_reset,
                queued_max_messages_kbytes=queued_max_messages_kbytes,
                queued_min_messages=queued_min_messages,
            ),
            processing_params=ProcessingParameters(
                processes=processes,
                input_block_size=input_block_size,
                output_block_size=output_block_size,
            ),
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            metrics=metrics,
            slice_id=instruction.slice_id,
            join_timeout=None,
        )

        consumer = consumer_builder.build_dlq_consumer(instruction)

        consumer.run()
        consumer_builder.flush()
        clear_instruction()
