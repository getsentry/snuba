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
    def __init__(self, gcs_bucket: Optional[GCSUploader] = None) -> None:
        self.uploader = None
        if gcs_bucket:
            try:
                self.uploader = GCSUploader(gcs_bucket)
            except Exception as e:
                logger.info(f"Couldn't set up gcs for bucket {gcs_bucket}: {str(e)}")

    def _format_filename(self, table: str, date: datetime) -> str:
        # Example: queries_2024_01_16_1_errors_local - first hour
        #          queries_2024_01_16_2_errors_local- second hour
        day = datetime.strftime(date, "%Y_%m_%d")
        hour = date.hour
        return f"queries_{day}_{hour}_{table}"

    def _format_blob_name(self, table: str, date: datetime) -> str:
        # Example: queries/2024_01_16/1/errors_local - first hour
        #          queries/2024_01_16/2/errors_local- second hour
        day = datetime.strftime(date, "%Y_%m_%d")
        hour = date.hour
        return f"queries/{day}/{hour}/{table}"

    def _full_path(self, filename) -> str:
        return f"/tmp/{filename}.csv"

    def _save_to_csv(self, filename: str, results: Sequence[QueryToRerun]) -> None:
        with open(self._full_path(filename), mode="w") as file:
            writer = csv.writer(file)
            for row in results:
                writer.writerow(row)

        logger.info(f"File {self._full_path(filename)} saved")

    def _save_to_gcs(self, filename: str, blob_name: str) -> None:
        self.uploader.upload_file(self._full_path(filename), blob_name)

    def save(self, table, date, results) -> None:
        filename = self._format_filename(table, date)
        self._save_to_csv(filename, results)
        if self.uploader:
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
    # todo: only putting this default for now
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
    We don't need the results of the query because we'll
    be using the query_log to compare results. We just
    need to replay all the queries and map the original
    query_id to the new query id so that when we compare
    results later we can make sure we are comparing the
    same query.
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

    def get_queries_from_querylog(table, start, end) -> Sequence[QueryToRerun]:
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
