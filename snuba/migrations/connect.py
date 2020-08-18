import logging
import time

from snuba.clusters.cluster import ClickhouseClientSettings, CLUSTERS


logger = logging.getLogger(__name__)


def check_clickhouse_connections() -> None:
    """
    Ensure that we can establish a connection with every cluster.
    """
    attempts = 0

    for cluster in CLUSTERS:
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

        while True:
            try:
                logger.debug(
                    "Attempting to connect to Clickhouse cluster %s (attempt %d)",
                    cluster,
                    attempts,
                )
                clickhouse.execute("SELECT 1")
                break
            except Exception as e:
                logger.error(
                    "Connection to Clickhouse cluster %s failed (attempt %d)",
                    cluster,
                    attempts,
                    exc_info=e,
                )
                attempts += 1
                if attempts == 60:
                    raise
                time.sleep(1)
