import logging
from typing import Optional, Sequence

import click

from snuba import settings
from snuba.datasets.factory import ACTIVE_DATASET_NAMES, get_dataset
from snuba.environment import clickhouse_rw, setup_logging
from snuba.migrations.migrate import run


@click.command()
@click.option(
    "--bootstrap-server",
    default=settings.DEFAULT_BROKERS,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option("--kafka/--no-kafka", default=True)
@click.option("--force", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
def bootstrap(
    *,
    bootstrap_server: Sequence[str],
    kafka: bool,
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
            writable_storage = dataset.get_writable_storage()

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

    attempts = 0
    while True:
        try:
            logger.debug("Attempting to connect to Clickhouse (attempt %d)", attempts)
            clickhouse_rw.execute("SELECT 1")
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
    existing_tables = {row[0] for row in clickhouse_rw.execute("show tables")}
    for name in ACTIVE_DATASET_NAMES:
        dataset = get_dataset(name)

        logger.debug("Creating tables for dataset %s", name)
        run_migrations = False
        for storage in dataset.get_all_storages():
            for statement in storage.get_schemas().get_create_statements():
                if statement.table_name not in existing_tables:
                    # This is a hack to deal with updates to Materialized views.
                    # It seems that ClickHouse would parse the SELECT statement that defines a
                    # materialized view even if the view already exists and the CREATE statement
                    # includes the IF NOT EXISTS clause.
                    # When we add a column to a matview, though, we will be in a state where, by
                    # running bootstrap, ClickHouse will parse the SQL statement to try to create
                    # the view and fail because the column does not exist yet on the underlying table,
                    # since the migration on the underlying table has not ran yet.
                    # Migrations are per dataset so they can only run after the bootstrap of an
                    # entire dataset has run. So we would have bootstrap depending on migration
                    # and migration depending on bootstrap.
                    # In order to break this dependency we skip bootstrap DDL calls here if the
                    # table/view already exists, so it is always safe to run bootstrap first.
                    logger.debug("Executing:\n%s", statement.statement)
                    clickhouse_rw.execute(statement.statement)
                else:
                    logger.debug("Skipping existing table %s", statement.table_name)
                    run_migrations = True
        if run_migrations:
            logger.debug("Running missing migrations for dataset %s", name)
            run(clickhouse_rw, dataset)
        logger.info("Tables for dataset %s created.", name)
