import logging
import time

from packaging import version

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, CLUSTERS
from snuba.migrations.clickhouse import CLICKHOUSE_SERVER_MIN_VERSION
from snuba.migrations.errors import InvalidClickhouseVersion

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
                check_clickhouse(clickhouse)
                break
            except InvalidClickhouseVersion as e:
                logger.error(e)
                raise
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


def check_clickhouse(clickhouse: ClickhousePool) -> None:
    ver = clickhouse.execute("SELECT version()")[0][0]
    if version.parse(ver) < version.parse(CLICKHOUSE_SERVER_MIN_VERSION):
        raise InvalidClickhouseVersion(
            f"Snuba requires Clickhouse version {CLICKHOUSE_SERVER_MIN_VERSION}"
        )
