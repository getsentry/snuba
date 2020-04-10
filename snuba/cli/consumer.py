import signal
from typing import Optional, Sequence

import click

from snuba import settings
from snuba.consumers.consumer_builder import ConsumerBuilder
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.factory import DATASET_NAMES, get_dataset
from snuba.environment import setup_logging, setup_sentry
from snuba.stateful_consumer.consumer_state_machine import ConsumerStateMachine


@click.command()
@click.option("--raw-events-topic", help="Topic to consume raw events from.")
@click.option(
    "--replacements-topic", help="Topic to produce replacement messages info.",
)
@click.option(
    "--commit-log-topic",
    help="Topic for committed offsets to be written to, triggering post-processing task(s)",
)
@click.option("--control-topic", help="Topic used to control the snapshot")
@click.option(
    "--consumer-group",
    default="snuba-consumers",
    help="Consumer group use for consuming the raw events topic.",
)
@click.option(
    "--bootstrap-server", multiple=True, help="Kafka bootstrap server to use.",
)
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
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
    "--stateful-consumer",
    default=False,
    type=bool,
    help="Runs a stateful consumer (that manages snapshots) instead of a basic one.",
)
@click.option(
    "--rapidjson-deserialize",
    default=False,
    type=bool,
    help="Uses rapidjson to deserialize messages",
)
@click.option(
    "--rapidjson-serialize",
    default=False,
    type=bool,
    help="Uses rapidjson to serialize messages",
)
def consumer(
    *,
    raw_events_topic: Optional[str],
    replacements_topic: Optional[str],
    commit_log_topic: Optional[str],
    control_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    dataset_name: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    stateful_consumer: bool,
    rapidjson_deserialize: bool,
    rapidjson_serialize: bool,
    log_level: Optional[str] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()

    dataset = get_dataset(dataset_name)

    consumer_builder = ConsumerBuilder(
        dataset_name=dataset_name,
        raw_topic=raw_events_topic,
        replacements_topic=replacements_topic,
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        bootstrap_servers=bootstrap_server,
        group_id=consumer_group,
        commit_log_topic=commit_log_topic,
        auto_offset_reset=auto_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
        rapidjson_deserialize=rapidjson_deserialize,
        rapidjson_serialize=rapidjson_serialize,
    )

    if stateful_consumer:
        storage = dataset.get_writable_storage()

        assert isinstance(
            storage, CdcStorage
        ), "Only CDC storages have a control topic thus are supported."
        context = ConsumerStateMachine(
            consumer_builder=consumer_builder,
            topic=control_topic or storage.get_default_control_topic(),
            group_id=consumer_group,
            storage=storage,
        )

        def handler(signum, frame) -> None:
            context.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        context.run()
    else:
        consumer = consumer_builder.build_base_consumer()

        def handler(signum, frame) -> None:
            consumer.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        consumer.run()
