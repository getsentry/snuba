import logging
from typing import Optional, Sequence

import click

from confluent_kafka import KafkaError, KafkaException
from snuba.datasets.factory import ACTIVE_DATASET_NAMES, get_dataset
from snuba.environment import setup_logging
from snuba.migrations.connect import check_clickhouse_connections
from snuba.migrations.runner import Runner
from snuba.utils.logging import pylog_to_syslog_level
from snuba.utils.streams.backends.kafka import get_default_kafka_configuration


@click.command()
@click.option(
    "--bootstrap-server",
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

        override_params = {
            # Same as above: override socket timeout as we expect Kafka
            # to not getting ready for a while
            "socket.timeout.ms": 1000,
        }
        if logger.getEffectiveLevel() != logging.DEBUG:
            # Override rdkafka loglevel to be critical unless we are
            # debugging as we expect failures when trying to connect
            # (Kafka may not be up yet)
            override_params["log_level"] = pylog_to_syslog_level(logging.CRITICAL)

        attempts = 0
        while True:
            try:
                logger.info("Attempting to connect to Kafka (attempt %d)...", attempts)
                client = AdminClient(
                    get_default_kafka_configuration(
                        bootstrap_servers=bootstrap_server,
                        override_params=override_params,
                    )
                )
                client.list_topics(timeout=1)
                break
            except KafkaException as err:
                logger.debug(
                    "Connection to Kafka failed (attempt %d)", attempts, exc_info=err
                )
                attempts += 1
                if attempts == 60:
                    raise
                time.sleep(1)

        logger.info("Connected to Kafka on attempt %d", attempts)

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

        logger.info("Creating Kafka topics...")
        for topic, future in client.create_topics(
            list(topics.values()), operation_timeout=1
        ).items():
            try:
                future.result()
                logger.info("Topic %s created", topic)
            except KafkaException as err:
                if err.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.error("Failed to create topic %s", topic, exc_info=err)

    if migrate:
        check_clickhouse_connections()
        Runner().run_all(force=True)
