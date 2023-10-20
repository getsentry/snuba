import csv
from typing import Optional, Sequence

import click
import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.clickhouse.span_cardinality_analyzer import (
    SpanGroupingCardinalityResult,
    span_grouping_cardinality_query,
    span_grouping_distinct_modules_query,
)
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)


def write_cardnaltiy_to_csv(
    results: Sequence[SpanGroupingCardinalityResult], filename: str
) -> None:
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(["org_id", "project_id", "category", "cardinality"])
        for row in results:
            writer.writerow(row)


@click.command()
@click.option("--log-level", help="Logging level to use.")
def spans_cardinality_analyzer(
    *,
    log_level: Optional[str] = None,
) -> None:
    """
    Analyze the cardinality of metrics extracted from spans. Intended to be run as a cron job.
    """
    setup_logging(log_level)
    setup_sentry()

    slack_client = SlackClient(
        settings.STARFISH_SLACK_CHANNEL_ID, settings.SLACK_API_TOKEN
    )

    storage_key = StorageKey("generic_metrics_distributions")
    storage = get_storage(storage_key)

    connection = storage.get_cluster().get_query_connection(
        ClickhouseClientSettings.CARDINALITY_ANALYZER
    )

    # Get the distinct span modules we are ingesting.
    result = connection.execute(
        span_grouping_distinct_modules_query(time_window_hrs=1),
    )
    distinct_span_groups = [row[0] for row in result.results]

    # Get the cardinality of each module and write to CSV file and send to Slack
    for span_group in distinct_span_groups:
        logger.info(f"Getting cardinality for span group: {span_group}")
        result = connection.execute(
            span_grouping_cardinality_query(span_group, time_window_hrs=24, limit=1000)
        )

        write_cardnaltiy_to_csv(result.results, f"/tmp/{span_group}.csv")
        slack_client.post_file(
            file_name=f"{span_group}.csv",
            file_path=f"/tmp/{span_group}.csv",
            file_type="csv",
            initial_comment=f"Span Grouping Cardinality Report: {span_group}",
        )
