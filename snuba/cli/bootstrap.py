import logging
from syslog import LOG_CRIT
from typing import Optional, Sequence

import click
from confluent_kafka import KafkaException

from snuba.clusters.cluster import CLUSTERS
from snuba.datasets.storages.factory import get_all_storage_keys
from snuba.environment import setup_logging
from snuba.migrations.connect import (
    check_clickhouse_connections,
    check_for_inactive_replicas,
)
from snuba.migrations.runner import Runner
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic


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
        from confluent_kafka.admin import AdminClient

        override_params = {
            # Same as above: override socket timeout as we expect Kafka
            # to not getting ready for a while
            "socket.timeout.ms": 1000,
        }
        if logger.getEffectiveLevel() != logging.DEBUG:
            # Override rdkafka loglevel to be critical unless we are
            # debugging as we expect failures when trying to connect
            # (Kafka may not be up yet)
            override_params["log_level"] = LOG_CRIT

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

        create_topics(client, [t for t in Topic])

    if migrate:
        check_clickhouse_connections(CLUSTERS)
        check_for_inactive_replicas(get_all_storage_keys())
        Runner().run_all(force=True)
