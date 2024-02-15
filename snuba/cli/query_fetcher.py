from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple

import click
import structlog

from snuba.clickhouse.native import ClickhousePool
from snuba.clickhouse.upgrades.comparisons import (
    FileFormat,
    FileManager,
    QueryInfoResult,
)
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)


def get_querylog_query(
    database: str, table: str, start: datetime, end: datetime
) -> str:
    start_time = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strftime(end, "%Y-%m-%d %H:%M:%S")

    return rf"""
    SELECT
        query_id,
        query
    FROM system.query_log
    WHERE (query_kind = 'Select')
    AND (type = 'QueryFinish')
    AND (has(databases, '{database}'))
    AND (has(tables, '{table}'))
    AND (query_start_time >= toDateTime('{start_time}'))
    AND (query_start_time <= toDateTime('{end_time}'))
    """


def get_credentials() -> Tuple[str, str]:
    # TOOO don't hardcode credentials, use settings
    return ("default", "")


@click.command()
@click.option(
    "--querylog-host",
    default="localhost",
    required=True,
    help="If querying directly which host for the querylog.",
)
@click.option(
    "--querylog-port",
    type=int,
    default=9000,
    required=True,
    help="If querying directly which port for the querylog",
)
@click.option(
    "--window-hours",
    type=int,
    default=24,
    required=True,
    help="Time window to re-run queries, in hours, max of 24",
)
@click.option(
    "--tables",
    type=str,
    required=True,
    help="table names separated with ,",
)
@click.option(
    "--database",
    type=str,
    default="default",
    help="Name of the ClickHouse database",
    required=True,
)
@click.option(
    "--gcs-bucket",
    help="Name of gcs bucket to save query files to",
)
@click.option("--log-level", help="Logging level to use.")
def query_fetcher(
    *,
    querylog_host: str,
    querylog_port: int,
    window_hours: int,
    tables: str,
    database: str,
    gcs_bucket: Optional[str],
    log_level: Optional[str] = None,
) -> None:
    """
    For a given number of hours (window_hours) in the past,
    fetch those queries run for each hour from the querylog.
    These queries will be saved to a GCS bucket for future
    use by the query_replayer cli command.

    e.g.
    Assuming window_hours is 2, table is `test_table`, and
    the job is running at 00:01:00 (random time in hour 0)

        Queries fetched (rounded to the hour):
        * 2hours ago - 1hour ago
        * 1hour ago - now

        will be saved as:
        queries/2024_01_16/test_table_22.csv - first hour
        queries/2024_01_16/test_table_23.csv - second hour

    """
    setup_logging(log_level)
    setup_sentry()

    if window_hours > 24:
        # enforce max of 24
        window_hours = 24

    uploader = GCSUploader(gcs_bucket)
    file_saver = FileManager(uploader)

    (clickhouse_user, clickhouse_password) = get_credentials()
    connection = ClickhousePool(
        host=querylog_host,
        port=querylog_port,
        user=clickhouse_user,
        password=clickhouse_password,
        database=database,
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )

    def get_queries_from_querylog(
        table: str, start: datetime, end: datetime
    ) -> Sequence[QueryInfoResult]:
        queries = []
        q = get_querylog_query(database, f"{database}.{table}", start, end)
        q_results = connection.execute(q)
        for querylog_data in q_results.results:
            query_id, query = querylog_data
            queries.append(QueryInfoResult(query_id=query_id, query_str=query))
        return queries

    table_names = [t for t in tables.split(",")]
    # rounded to the hour
    now = datetime.utcnow().replace(
        microsecond=0,
        second=0,
        minute=0,
    )
    window_hours_ago_ts = now - timedelta(hours=window_hours)
    interval = timedelta(hours=1)

    start_time = window_hours_ago_ts
    for table in table_names:
        while start_time < now:
            end_time = start_time + interval
            logger.info(f"Fetching queries to run from {table}...")
            queries = get_queries_from_querylog(table, start_time, end_time)
            file_format = FileFormat(
                directory="queries", date=start_time, table=table, hour=start_time.hour
            )
            file_saver.save(file_format, queries)
            logger.info(f"Saved {len(queries)} queries from {table}")
            start_time = end_time
