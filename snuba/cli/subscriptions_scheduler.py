import logging
import os
import signal
from contextlib import closing
from typing import Any, Optional

import click
from arroyo import Topic, configure_metrics
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.abstract import TopicExists
from arroyo.backends.local.storages.file import FileMessageStorage

from snuba import environment, settings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.environment import setup_logging, setup_sentry
from snuba.subscriptions.scheduler_consumer import SchedulerBuilder
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--entity",
    "entity_name",
    required=True,
    type=click.Choice(
        ["events", "transactions", "sessions", "metrics_sets", "metrics_counters"]
    ),
    help="The entity to target",
)
@click.option(
    "--consumer-group",
    default="snuba-subscription-scheduler",
    help="Consumer group used for consuming the commit log topic.",
)
@click.option(
    "--followed-consumer-group",
    required=True,
    help="Name of the consumer group to follow",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option("--schedule-ttl", type=int, default=60 * 5)
@click.option("--log-level", help="Logging level to use.")
@click.option("--delay-seconds", type=int)
@click.option(
    "--profile-path", type=click.Path(dir_okay=True, file_okay=False, exists=True)
)
@click.option(
    "--load-factor",
    type=int,
    default=settings.SUBSCRIPTIONS_SCHEDULER_LOAD_FACTOR,
    help="Temporary option to simulate additional load. To be removed after testing.",
)
def subscriptions_scheduler(
    *,
    entity_name: str,
    consumer_group: str,
    followed_consumer_group: str,
    auto_offset_reset: str,
    schedule_ttl: int,
    log_level: Optional[str],
    delay_seconds: Optional[int],
    profile_path: Optional[str],
    load_factor: int,
) -> None:
    """
    The subscriptions scheduler's job is to schedule subscriptions for a single entity.
    It consumes the commit log for that entity which is used as a clock and determines
    which subscriptions to run at each interval. It produces a message for each
    scheduled subscription task to the scheduled subscription topic for that entity, so
    it can be picked up and run by subscription executors.

    The subscriptions scheduler consists of a tick consumer and three processing steps.

    - The tick consumer consumes the commit log and reads the "orig_message_ts" header.
    It constructs a new `Tick` message representing the intervals between each of the
    original messages, which gets passed to the processing strategy. Note: A tick always
    corresponds to a single partition on the original topic (not the commit log topic
    as that is never partitioned).

    - The first processing step is a tick buffer. It buffers ticks where needed and
    determines when to submit them to the rest of the pipeline. The tick buffer behavior
    depends on the watermark mode specified by the entity. In PARTITION mode, ticks are
    never buffered and immediately submitted to the next step. In GLOBAL mode we wait
    (filling the buffer) until the timestamp of a tick has been reached on every
    partition before eventually submitting a tick to the next step. This guarantees that
    a subscription is never scheduled before data on every partition up to that
    timestamp is written to storage.

    - The second processing step provides the strategy for committing offsets. Ticks are
    only marked as `should_commit` if every partition has already reached the timestamp
    of the tick. Only the commit log offset of the slowest partition (on the main topic)
    will get committed. This guarantees at least once scheduling of subscriptions.

    - The third processing step checks the subscription store to determine which
    subscriptions need to be scheduled for each tick. Each scheduled subscription task
    is encoded and produced to the scheduled topic. Offsets are commited if the
    `should_commit` value provided by the previous strategy is true, and only once all
    prior scheduled subscriptions were succesfully produced (and replicated).
    """

    setup_logging(log_level)
    setup_sentry()

    metrics = MetricsWrapper(
        environment.metrics, "subscriptions.scheduler", tags={"entity": entity_name}
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    entity_key = EntityKey(entity_name)

    storage = get_entity(entity_key).get_writable_storage()

    assert (
        storage is not None
    ), f"Entity {entity_name} does not have a writable storage by default."

    stream_loader = storage.get_table_writer().get_stream_loader()

    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None

    directory_path = os.getcwd() + "/.broker_data"
    broker: Broker[KafkaPayload] = Broker(FileMessageStorage(directory_path))

    # Create the scheduled topic if it doesn't exist
    topic = Topic(scheduled_topic_spec.topic_name)
    try:
        broker.create_topic(topic, partitions=1)
    except TopicExists:
        pass

    producer = broker.get_producer()

    builder = SchedulerBuilder(
        broker,
        entity_name,
        consumer_group,
        followed_consumer_group,
        producer,
        auto_offset_reset,
        schedule_ttl,
        delay_seconds,
        metrics,
        profile_path,
        # TODO: Just for testing, should be removed before the scheduler is actually used
        load_factor,
    )

    processor = builder.build_consumer()

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    with closing(producer):
        processor.run()
