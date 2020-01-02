import logging
import click
from typing import Sequence

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES


@click.command()
@click.option(
    "--bootstrap-server",
    default=settings.DEFAULT_BROKERS,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option("--kafka/--no-kafka", default=True)
@click.option("--force", is_flag=True)
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
def bootstrap(
    *, bootstrap_server: Sequence[str], kafka: bool, force: bool, log_level: str
) -> None:
    """
    Warning: Not intended to be used in production yet.
    """
    if not force:
        raise click.ClickException("Must use --force to run")

    logger = logging.getLogger("snuba.bootstrap")
    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )

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

        topics = []
        for name in DATASET_NAMES:
            dataset = get_dataset(name)
            table_writer = dataset.get_table_writer()
            if table_writer:
                stream_loader = table_writer.get_stream_loader()
                for topic_spec in stream_loader.get_all_topic_specs():
                    topics.append(
                        NewTopic(
                            topic_spec.topic_name,
                            num_partitions=topic_spec.partitions_number,
                            replication_factor=topic_spec.replication_factor,
                        )
                    )

        for topic, future in client.create_topics(topics).items():
            try:
                future.result()
                logger.info("Topic %s created", topic)
            except Exception as e:
                logger.error("Failed to create topic %s", topic, exc_info=e)

    from snuba.clickhouse.native import ClickhousePool

    attempts = 0
    while True:
        try:
            logger.debug("Attempting to connect to Clickhouse (attempt %d)", attempts)
            ClickhousePool().execute("SELECT 1")
            break
        except Exception as e:
            logger.error(
                "Connection to Clickhouse failed (attempt %d)", attempts, exc_info=e
            )
            attempts += 1
            if attempts == 60:
                raise
            time.sleep(1)

    # Need to better figure out if we are configured to use replicated
    # tables or distributed tables, etc.

    # Create the tables for every dataset.
    for name in DATASET_NAMES:
        dataset = get_dataset(name)

        logger.debug("Creating tables for dataset %s", name)
        for statement in dataset.get_dataset_schemas().get_create_statements():
            logger.debug("Executing:\n%s", statement)
            ClickhousePool().execute(statement)
        logger.info("Tables for dataset %s created.", name)
