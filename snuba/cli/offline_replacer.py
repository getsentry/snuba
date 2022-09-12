import logging
import time
from datetime import datetime
from typing import Optional

import click
import simplejson as json
from arroyo import Message
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import Partition, Topic

from snuba import environment
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.replacer import ReplacerWorker
from snuba.utils.metrics.wrapper import MetricsWrapper


@click.command()
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["errors", "errors_v2"]),
    help="The storage to consume/run replacements for (currently only events supported)",
    required=True,
)
@click.option(
    "--file",
    "file_name",
    help="The file name containing the replacement to be applied",
    required=True,
)
@click.option(
    "--project",
    "project_id",
    type=int,
    help="The project to execute",
    required=True,
)
@click.option("--dry-run", is_flag=True)
@click.option(
    "--start-from",
    "start_from",
    type=int,
    help="Offset in lines from the beginning of the file",
    default=0,
)
@click.option(
    "--delay",
    "delay",
    type=int,
    help="Waiting time between two replacements in seconds",
    default=0,
)
@click.option("--log-level", help="Logging level to use.")
def offline_replacer(
    *,
    storage_name: str,
    file_name: str,
    project_id: int,
    dry_run: bool,
    start_from: int,
    delay: int,
    log_level: Optional[str] = None,
) -> None:
    """
    Executes a list of replacements taken from a file instead of taking
    them from Kafka.

    Example of the format:
    [
        2,
        "start_delete_tag",
        {
            "transaction_id":"34b07c124e964f5b9e510edb.....",
            "project_id":123123123,
            "tag":"tag_name",
            "datetime":"2022-07-08T08:28:21.537918Z"
        }
    ]
    """
    logger = logging.getLogger(__name__)
    setup_logging(log_level)
    setup_sentry()

    metrics = MetricsWrapper(environment.metrics, "errors.offline_replacer")

    storage_key = StorageKey(storage_name)
    storage = get_writable_storage(storage_key)
    worker = ReplacerWorker(storage, "noop", metrics)
    current_offset = 0
    with open(file_name) as f:
        for line in f:
            if current_offset <= start_from:
                current_offset += 1
                continue
            current_offset += 1

            message = json.loads(line)
            logger.info(message)
            [_, action_type, data] = message
            replace_project = data["project_id"]
            if replace_project != project_id:
                logger.info(f"Offset {current_offset} [Skip project] Skipping {data}")
                continue

            kafka_msg: Message[KafkaPayload] = Message(
                Partition(Topic("na"), 0),
                current_offset,
                KafkaPayload(None, line.encode(), []),
                datetime.now(),
            )
            replacement = worker.process_message(kafka_msg)
            if replacement is not None:
                logger.debug(f"Replacement to process {replacement}")
                if not dry_run:
                    worker.flush_batch([replacement])
                    logger.info(
                        f"Offset {current_offset} [OK] Processing action {action_type} data: {data}"
                    )
                else:
                    logger.info(
                        f"Offset {current_offset} [Pretending] Processing action {action_type} data: {data}"
                    )
            else:
                logger.info(
                    f"Offset {current_offset} [No action] Processing action {action_type} data: {data}"
                )
            if delay > 0:
                time.sleep(delay)
