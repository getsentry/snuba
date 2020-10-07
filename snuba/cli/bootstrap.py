import logging
from typing import Optional, Sequence

import click

from snuba import settings
from snuba.datasets.factory import ACTIVE_DATASET_NAMES, get_dataset
from snuba.environment import setup_logging
from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.runner import Runner


@click.command()
@click.option(
    "--bootstrap-server",
    default=settings.DEFAULT_BROKERS,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option("--kafka/--no-kafka", default=True)
@click.option("--migrate/--no-migrate", default=True)
@click.option("--force", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
def bootstrap(
    *,
    bootstrap_server: Sequence[str],
    kafka: bool,
    migrate: bool,
    force: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    Warning: Not intended to be used in production yet.
    """
    if not force:
        raise click.ClickException("Must use --force to run")

    setup_logging(log_level)

    logger = logging.getLogger("snuba.bootstrap")

    import time

    if kafka:
        logger.debug("Using Kafka with %r", bootstrap_server)
        from confluent_kafka.admin import AdminClient, NewTopic

        attempts = 0
        while True:
            try:
                logger.debug("Attempting to connect to Kafka (attempt %d)", attempts)
                client = AdminClient(
                    {
                        "bootstrap.servers": ",".join(bootstrap_server),
                        "socket.timeout.ms": 1000,
                    }
                )
                client.list_topics(timeout=1)
                break
            except Exception as e:
                logger.error(
                    "Connection to Kafka failed (attempt %d)", attempts, exc_info=e
                )
                attempts += 1
                if attempts == 60:
                    raise
                time.sleep(1)

        topics = {}
        for name in ACTIVE_DATASET_NAMES:
            dataset = get_dataset(name)
            for entity in dataset.get_all_entities():
                writable_storage = entity.get_writable_storage()
                if writable_storage:
                    table_writer = writable_storage.get_table_writer()
                    stream_loader = table_writer.get_stream_loader()
                    for topic_spec in stream_loader.get_all_topic_specs():
                        if topic_spec.topic_name in topics:
                            continue
                        logger.debug(
                            "Adding topic %s to creation list", topic_spec.topic_name
                        )
                        topics[topic_spec.topic_name] = NewTopic(
                            topic_spec.topic_name,
                            num_partitions=topic_spec.partitions_number,
                            replication_factor=topic_spec.replication_factor,
                        )

        logger.debug("Initiating topic creation")
        for topic, future in client.create_topics(
            list(topics.values()), operation_timeout=1
        ).items():
            try:
                future.result()
                logger.info("Topic %s created", topic)
            except Exception as e:
                logger.error("Failed to create topic %s", topic, exc_info=e)

    if migrate:
        check_clickhouse_connections()
        Runner().run_all(force=True)
