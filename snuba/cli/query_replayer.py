from typing import Optional, Sequence

import click
import structlog

from collections import namedtuple
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)

QueryToRerun = namedtuple("Query", ["query_str", "query_id"])


def get_original_queries() -> Sequence[QueryToRerun]:
    # should probs get this from some file output somwhere
    # and parse it but for testing purposes hardcoding it
    return [
        QueryToRerun("select * from test_table", "655d5822-c88c-4b92-90bb-679d7e229671")
    ]


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to write to.",
    required=True,
)
@click.option(
    "--clickhouse-port",
    type=int,
    help="Clickhouse native port to write to.",
    required=True,
)
@click.option(
    "--filename",
    help="Filename for mapped query ids.",
    default="mapped_query_ids",
    required=False,
)
@click.option("--log-level", help="Logging level to use.")
def query_replayer(
    *,
    clickhouse_host: str,
    clickhouse_port: int,
    filename: Optional[str],
    log_level: Optional[str] = None,
) -> None:
    """
    We don't need the results of the query because we'll
    be using the query_log to compare results. We just
    need to replay all the queries and map the original
    query_id to the new query id so that when we compare
    results later we can make sure we are comparing the
    same query.
    """
    setup_logging(log_level)
    setup_sentry()

    connection = ClickhousePool(
        host=clickhouse_host,
        port=clickhouse_port,
        user="default",  # todo
        password="",  # todo
        database="default",  # todo
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )

    for q in get_original_queries():
        try:
            connection.execute(
                q.query_str,
                query_id=q.query_id,
            )
        except Exception:
            # todo(handle errors/do retries?)
            return
