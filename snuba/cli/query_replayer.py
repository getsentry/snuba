from collections import namedtuple
from typing import Optional, Sequence

import click
import csv
import structlog

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)

QueryToRerun = namedtuple("QueryToRerun", ["query_str", "query_id"])


def get_queries_from_file(filename: str) -> Sequence[QueryToRerun]:
    """
    Assumes there is a local csv file first, if not attempts to
    get a file from GCS.
    """

    def get_from_gcs(filename: str) -> Sequence[QueryToRerun]:
        # TODO if needed
        return []

    queries: Sequence[QueryToRerun] = []
    try:
        base = open(filename)
        base_reader = csv.reader(base)
        queries = [QueryToRerun(row[0], row[1]) for row in base_reader]
    except FileNotFoundError:
        logger.debug("No local CSV file")
        queries = get_from_gcs(filename)

    return queries


def save_to_csv(results: Sequence[QueryToRerun], filename: str) -> None:
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        for row in results:
            writer.writerow(row)


def get_querylog_query(
    databases: list[str], tables: list[str], window: Optional[int]
) -> str:
    if not window:
        window = 3600
    start = f"toDateTime(now() - {window})"

    return rf"""
    SELECT
        query_id,
        query
    FROM system.query_log
    WHERE (query_kind = 'Select')
    AND (type = 'QueryFinish')
    AND (databases = {databases})
    AND (tables = {tables})
    AND (query_start_time >= {start})
    AND (query_start_time <= toDateTime(now()))
    LIMIT 100
    """


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
    help="If querying direcly, file to save to and replay from, otherwise just to replay from",
    required=True,
    default="/tmp/queries_to_rerun.csv",
)
@click.option(
    "--querylog-host",
    help="If querying directly which host for the querylog.",
)
@click.option(
    "--querylog-port",
    type=int,
    help="If querying directly which port for the querylog",
)
@click.option(
    "--window",
    type=int,
    help="Time window to re-run queries, in seconds",
)
@click.option(
    "--storage-name",
    type=str,
    help="If querying directly which port for the querylog",
)
@click.option("--log-level", help="Logging level to use.")
def query_replayer(
    *,
    clickhouse_host: str,
    clickhouse_port: int,
    filename: str,
    querylog_host: Optional[str],
    querylog_port: Optional[int],
    window: Optional[int],
    storage_name: Optional[str],
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

    if querylog_host and querylog_port and storage_name:
        logger.info("Fetching queries to run from ClickHouse...")
        storage_key = StorageKey(storage_name)
        storage = get_storage(storage_key)
        (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()
        db = storage.get_cluster().get_database()
        connection = ClickhousePool(
            host=querylog_host,
            port=querylog_port,
            user=clickhouse_user,
            password=clickhouse_password,
            database=db,
            client_settings=ClickhouseClientSettings.QUERY.value.settings,
        )
        table_names = connection.execute("SHOW TABLES")

        def get_queries_from_querylog() -> Sequence[QueryToRerun]:
            queries = []
            for (table,) in table_names.results:
                q = get_querylog_query([db], [f"{db}.{table}"], window)
                q_results = connection.execute(q)
                for querylog_data in q_results.results:
                    query_id, query = querylog_data
                    queries.append(QueryToRerun(query_id=query_id, query_str=query))
            return queries

        queries = get_queries_from_querylog()
        save_to_csv(queries, filename)
    else:
        queries = get_queries_from_file(filename)

    connection = ClickhousePool(
        host=clickhouse_host,
        port=clickhouse_port,
        user="default",  # todo
        password="",  # todo
        database="default",  # todo
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )

    reran_queries = 0
    logger.info("Re-running queries...")
    for q in queries:
        try:
            connection.execute(
                q.query_str,
                query_id=q.query_id,
            )
            reran_queries += 1
        except Exception:
            logger.debug(
                f"Re-ran {reran_queries} queries before failing on {q.query_id}"
            )
            return

    logger.info(f"Successfully re-ran {reran_queries} queries")
