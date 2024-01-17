import csv
from datetime import datetime
from typing import NamedTuple, Optional, Sequence, Tuple

import click
import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)


class QueryMeasurementResult(NamedTuple):
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int
    read_rows: int
    read_bytes: int


def write_querylog_results_to_csv(
    results: Sequence[QueryMeasurementResult], filename: str
) -> None:
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "query_id",
                "query_duration_ms",
                "result_rows",
                "result_bytes",
                "read_rows",
                "read_bytes",
            ]
        )
        for row in results:
            writer.writerow(row)


def format_filename(table: str) -> str:
    # Example: errors_local_2024_01_16_130725
    now = datetime.now()
    day = datetime.strftime(now, "%Y-%m-%d").replace("-", "_")
    timestamp = datetime.strftime(now, "%H%M%S")
    return f"{table}_{day}_{timestamp}"


def get_query_results(
    type: str,
    databases: list[str],
    tables: list[str],
    start_time: str,
    end_time: Optional[str],
) -> str:
    if start_time and end_time:
        start = f"toDateTime('{start_time}')"
        end = f"toDateTime('{end_time}')"
    elif start_time and not end_time:
        start = f"toDateTime('{start_time}')"
        end = "toDateTime(now())"

    return rf"""
    SELECT
        query_id,
        query_duration_ms,
        result_rows,
        result_bytes,
        read_rows,
        read_bytes
    FROM system.query_log
    WHERE (query_kind = 'Select')
    AND (type = '{type}')
    AND (databases = {databases})
    AND (tables = {tables})
    AND (query_start_time >= {start})
    AND (query_start_time <= {end})
    ORDER BY query_id
    """


def get_credentials() -> Tuple[str, str]:
    # TOOO don't hardcode credentials, use settings
    return ("default", "")


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to write to.",
    required=True,
    default="localhost",
)
@click.option(
    "--clickhouse-port",
    type=int,
    help="Clickhouse native port to write to.",
    required=True,
    default=9000,
)
@click.option(
    "--database",
    help="ClickHouse Database to query from.",
    required=True,
    default="default",
)
@click.option(
    "--table",
    help="ClickHouse tables to query from.",
    required=True,
)
@click.option(
    "--event-type",
    help="Type of event that occured while executing query.",
    type=click.Choice(
        ["QueryFinish", "ExceptionBeforeStart", "ExceptionWhileProcessing"]
    ),
    required=True,
    default="QueryFinish",
)
@click.option(
    "--start-time",
    help="Starting timestamp for the query",
    required=True,
)
@click.option(
    "--notify",
    help="Option to send saved csv file to slack",
    is_flag=True,
    default=False,
)
@click.option(
    "--end-time",
    help="Ending timestamp of the query",
)
@click.option("--log-level", help="Logging level to use.")
def querylog_to_csv(
    *,
    clickhouse_host: str,
    clickhouse_port: int,
    table: str,
    database: str,
    event_type: str,
    start_time: str,
    notify: bool,
    end_time: Optional[str] = None,
    log_level: Optional[str] = None,
) -> None:
    """
    Use this command when you want to capture the results from the
    clickhouse querylog after you have re-run queries (using the
    `snuba query-replayer` command).
    """
    setup_logging(log_level)
    setup_sentry()

    # clickhouse expects tables like -> ['default.errors_local']
    tables = [f"{database}.{table}"]

    query = get_query_results(event_type, [database], tables, start_time, end_time)

    (clickhouse_user, clickhouse_password) = get_credentials()
    connection = ClickhousePool(
        host=clickhouse_host,
        port=clickhouse_port,
        user=clickhouse_user,
        password=clickhouse_password,
        database=database,
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )
    results = connection.execute(query)
    filename = format_filename(table)

    # TODO: save this file to GCS bucket
    write_querylog_results_to_csv(results.results, f"/tmp/{filename}.csv")

    if notify:
        # TODO: maybe use new specific channel id
        slack_client = SlackClient(
            channel_id=settings.SNUBA_SLACK_CHANNEL_ID, token=settings.SLACK_API_TOKEN
        )

        slack_client.post_file(
            file_name=f"{filename}.csv",
            file_path=f"/tmp/{filename}.csv",
            file_type="csv",
            initial_comment=f"Querylog Result Report: {filename}",
        )
