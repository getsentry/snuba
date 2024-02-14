import csv
from collections import namedtuple
from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple

import click
import structlog

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)

QueryToRerun = namedtuple("QueryToRerun", ["query_str", "query_id"])


class QueriesFileSaver:
    def __init__(self, gcs_bucket: Optional[str] = None) -> None:
        self.uploader = None
        if gcs_bucket:
            try:
                self.uploader = GCSUploader(gcs_bucket)
            except Exception as e:
                logger.info(f"Couldn't set up gcs for bucket {gcs_bucket}: {str(e)}")

    def _format_filename(self, table: str, date: datetime) -> str:
        # Example: queries_2024_01_16_errors_local_1 - first hour
        #          queries_2024_01_16_errors_local_2 - second hour
        day = datetime.strftime(date, "%Y_%m_%d")
        hour = date.hour
        return f"queries_{day}_{table}_{hour}"

    def _format_blob_name(self, table: str, date: datetime) -> str:
        # Example: queries/2024_01_16/errors_local_1 - first hour
        #          queries/2024_01_16/errors_local_2- second hour
        day = datetime.strftime(date, "%Y_%m_%d")
        hour = date.hour
        return f"queries/{day}/{table}_{hour}.csv"

    def _full_path(self, filename: str) -> str:
        return f"/tmp/{filename}.csv"

    def _save_to_csv(self, filename: str, results: Sequence[QueryToRerun]) -> None:
        with open(self._full_path(filename), mode="w") as file:
            writer = csv.writer(file)
            for row in results:
                writer.writerow(row)

        logger.info(f"File {self._full_path(filename)} saved")

    def _save_to_gcs(self, filename: str, blob_name: str) -> None:
        if self.uploader:
            self.uploader.upload_file(self._full_path(filename), blob_name)

    def save(self, table: str, date: datetime, results: Sequence[QueryToRerun]) -> None:
        """
        First save the results to local csv file,
        then upload the file to gcs bucket.
        """
        filename = self._format_filename(table, date)
        self._save_to_csv(filename, results)

        blob_name = self._format_blob_name(table, date)
        self._save_to_gcs(filename, blob_name)


def get_querylog_query(
    databases: list[str], tables: list[str], start: datetime, end: datetime
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
    AND (databases = {databases})
    AND (tables = {tables})
    AND (query_start_time >= toDateTime('{start_time}'))
    AND (query_start_time <= toDateTime('{end_time}'))
    LIMIT 100
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
        queries/2024_01_16/test_table_22.csv - second hour

    """
    setup_logging(log_level)
    setup_sentry()

    if window_hours > 24:
        # enforce max of 24
        window_hours = 24

    file_saver = QueriesFileSaver(gcs_bucket=gcs_bucket)

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
    ) -> Sequence[QueryToRerun]:
        queries = []
        q = get_querylog_query([database], [f"{database}.{table}"], start, end)
        q_results = connection.execute(q)
        for querylog_data in q_results.results:
            query_id, query = querylog_data
            queries.append(QueryToRerun(query_id=query_id, query_str=query))
        return queries

    table_names = [t for t in tables.split(",")]
    now = datetime.utcnow()
    window_hours_ago_ts = now.replace(
        microsecond=0,
        second=0,
        minute=0,
    ) - timedelta(hours=window_hours)
    interval = timedelta(hours=1)

    start_time = window_hours_ago_ts
    for table in table_names:
        while start_time < now:
            end_time = start_time + interval
            logger.info(f"Fetching queries to run from {table}...")
            queries = get_queries_from_querylog(table, start_time, end_time)
            file_saver.save(table, start_time, queries)
            logger.info(f"Saved {len(queries)} queries from {table}")
            start_time = end_time
