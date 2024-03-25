import time
from datetime import datetime
from typing import Optional, Tuple

import click
import sentry_sdk
import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.clickhouse.native import ClickhousePool
from snuba.clickhouse.upgrades.comparisons import (
    BlobGetter,
    FileFormat,
    FileManager,
    QueryInfoResult,
    QueryMeasurementResult,
)
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)


def format_results_query(
    type: str,
    database: str,
    table: str,
    start: datetime,
    end: datetime,
) -> str:
    start_time = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strftime(end, "%Y-%m-%d %H:%M:%S")
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
    AND (has(databases, '{database}'))
    AND (has(tables, '{table}'))
    AND (query_start_time >= '{start_time}')
    AND (query_start_time <= '{end_time}')
    ORDER BY query_id
    """


def get_credentials(user: Optional[str], password: Optional[str]) -> Tuple[str, str]:
    # TOOO don't hardcode credentials, use settings
    return (user or "default", password or "")


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to write to.",
    default="localhost",
    required=True,
)
@click.option(
    "--clickhouse-port",
    type=int,
    default=9000,
    help="Clickhouse native port to write to.",
    required=True,
)
@click.option(
    "--clickhouse-user",
    help="Clickhouse user to authenticate with.",
    default=None,
    required=False,
)
@click.option(
    "--clickhouse-password",
    help="Clickhouse password to authenticate with.",
    default=None,
    required=False,
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
    "--override",
    help="Option to override any previously re-run results",
    is_flag=True,
    default=False,
)
@click.option(
    "--notify",
    help="Option to send saved csv file to slack",
    is_flag=True,
    default=False,
)
@click.option(
    "--gcs-bucket",
    help="Name of gcs bucket to save query files to",
    required=True,
)
@click.option(
    "--wait-seconds",
    help="Number of seconds to wait between re-running queries and getting the results.",
    type=int,
    required=True,
    default=30,
)
@click.option("--log-level", help="Logging level to use.")
def query_replayer(
    *,
    clickhouse_host: str,
    clickhouse_port: int,
    event_type: str,
    override: bool,
    notify: bool,
    gcs_bucket: str,
    wait_seconds: int,
    log_level: Optional[str] = None,
    clickhouse_user: Optional[str] = None,
    clickhouse_password: Optional[str] = None,
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

    database = "default"  # todo
    (clickhouse_user, clickhouse_password) = get_credentials(
        clickhouse_user, clickhouse_password
    )
    connection = ClickhousePool(
        host=clickhouse_host,
        port=clickhouse_port,
        user=clickhouse_user,
        password=clickhouse_password,
        database=database,
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )

    def get_version() -> str:
        [(version,)] = connection.execute("SELECT version()").results
        major, minor, _ = version.split(".", 2)
        return f"{major}-{minor}"

    uploader = GCSUploader(gcs_bucket)
    blob_getter = BlobGetter(uploader)
    file_manager = FileManager(uploader)

    results_directory = f"results-{get_version()}"
    if override:
        blobs_to_replay = sorted(blob_getter.get_all_names(prefix="queries"))
    else:
        blobs_to_replay = sorted(
            blob_getter.get_name_diffs(("queries/", f"{results_directory}/"))
        )

    def rerun_queries_for_blob(blob: str) -> Tuple[int, int]:
        queries = file_manager.download(blob)
        reran_queries = 0
        total_queries = len(queries)
        logger.info(f"Re-running queries for {blob}")
        for q in queries:
            assert isinstance(q, QueryInfoResult)
            try:
                connection.execute(
                    q.query_str,
                    query_id=q.query_id,
                )
                reran_queries += 1
            except Exception as e:
                logger.info(
                    f"Re-ran {reran_queries}/{total_queries} queries before failing on {q.query_id}"
                )
                # capturing the execption so that we can debug,
                # but not re-raising because we don't want one
                # blob to prevent others from being processed
                sentry_sdk.capture_exception(e)
        logger.info(f"Re-ran {reran_queries}/{total_queries} queries")
        return (total_queries, reran_queries)

    for blob_name in blobs_to_replay:
        # adding buffer around querylog query
        time.sleep(1)
        rerun_start = datetime.utcnow()
        total, reran = rerun_queries_for_blob(blob_name)
        rerun_end = datetime.utcnow()

        if total == 0:
            logger.info(f"No queries to re-run for {blob_name}")
            continue

        if reran != total:
            logger.info(f"Incomplete re-run for {blob_name}")
            continue

        queries_file_format = file_manager.parse_blob_name(blob_name)
        query = format_results_query(
            event_type,
            database,
            f"{database}.{queries_file_format.table}",
            rerun_start,
            rerun_end,
        )
        # make sure there is enough time for the replayed queries to finish
        # otherwise their results won't be captured
        time.sleep(wait_seconds)
        results = connection.execute(query)

        replay_results = []
        for replay_result in results.results:
            (
                query_id,
                query_duration_ms,
                result_rows,
                result_bytes,
                read_rows,
                read_bytes,
            ) = replay_result
            replay_results.append(
                QueryMeasurementResult(
                    query_id=query_id,
                    query_duration_ms=query_duration_ms,
                    result_rows=result_rows,
                    result_bytes=result_bytes,
                    read_rows=read_rows,
                    read_bytes=read_bytes,
                )
            )

        # File format is the same except for the directory
        result_file_format = FileFormat(
            directory=results_directory,
            date=queries_file_format.date,
            table=queries_file_format.table,
            hour=queries_file_format.hour,
        )
        file_manager.save(
            result_file_format,
            replay_results,
        )

        if notify:
            # TODO: maybe use new specific channel id
            filename = file_manager.format_filename(result_file_format)
            slack_client = SlackClient(
                channel_id=settings.SNUBA_SLACK_CHANNEL_ID,
                token=settings.SLACK_API_TOKEN,
            )

            slack_client.post_file(
                file_name=f"{filename}",
                file_path=f"/tmp/{filename}",
                file_type="csv",
                initial_comment=f"Querylog Result Report: {filename}",
            )

    # clear out the query_log table after we re-ran queries
    # connection.execute("TRUNCATE TABLE system.query_log")
