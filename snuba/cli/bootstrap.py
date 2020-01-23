import logging
import click
from typing import Sequence

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES
from snuba.migrate import run


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

        topics = {}
        for name in DATASET_NAMES:
            dataset = get_dataset(name)
            table_writer = dataset.get_table_writer()
            if table_writer:
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

    from snuba.clickhouse.native import ClickhousePool

    attempts = 0
    conn = ClickhousePool()
    while True:
        try:
            logger.debug("Attempting to connect to Clickhouse (attempt %d)", attempts)
            conn.execute("SELECT 1")
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
    existing_tables = {row[0] for row in conn.execute("show tables")}
    for name in DATASET_NAMES:
        dataset = get_dataset(name)

        logger.debug("Creating tables for dataset %s", name)
        run_migrations = False
        for statement in dataset.get_dataset_schemas().get_create_statements():
            if statement.table_name not in existing_tables:
                # This is a hack to deal with Materialized views.
                # All our DDL create statements are in the for of CREATE IF NOT EXISTS
                # This works well for all tables, except for Materialized views.
                # It seems Clockhouse parses the SQL query that defines the view even if
                # the materialized view already exists.
                # This breaks if we are making a change to the mat view schema.
                # In that case we cannot run the CREATE statement if the matview already
                # exists before we ensure all migrations have ran.
                logger.debug("Executing:\n%s", statement.statement)
                conn.execute(statement.statement)
            else:
                logger.debug("Skipping existing table %s", statement.table_name)
                run_migrations = True
        if run_migrations:
            logger.debug("Running missing migrations for dataset %s", name)
            run(conn, dataset)
        logger.info("Tables for dataset %s created.", name)
