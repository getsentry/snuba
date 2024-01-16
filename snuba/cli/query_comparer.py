import csv
from typing import Any, Optional, Sequence

import click
import structlog
import itertools
from dataclasses import dataclass

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient

from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)


@dataclass(frozen=True)
class QueryMeasurement:
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int
    read_rows: int
    read_bytes: int


@dataclass(frozen=True)
class MismatchedValues:
    base_value: int
    new_value: int
    delta: int


@dataclass
class QueryMismatch:
    query_id: str
    query_duration_ms: MismatchedValues
    result_rows: MismatchedValues
    result_bytes: MismatchedValues
    read_rows: MismatchedValues
    read_bytes: MismatchedValues

    def performance_mismatches(self):
        return (
            self.query_id,
            self.query_duration_ms.base_value,
            self.query_duration_ms.new_value,
            self.read_rows.base_value,
            self.read_rows.new_value,
            self.read_bytes.base_value,
            self.read_bytes.new_value,
            self.query_duration_ms.delta,
            self.read_rows.delta,
            self.read_bytes.delta,
        )

    def data_mismatches(self):
        return (
            self.query_id,
            self.result_rows.base_value,
            self.result_rows.new_value,
            self.result_bytes.base_value,
            self.result_bytes.new_value,
            self.result_rows.delta,
            self.result_bytes.delta,
        )


MEASUREMENTS = [
    "query_duration_ms",
    "result_rows",
    "result_bytes",
    "read_rows",
    "read_bytes",
]


def write_querylog_comparison_results_to_csv(
    results: Sequence[Any],
    filename: str,
    header_row=Sequence[str],
) -> None:
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(header_row)
        for row in results:
            writer.writerow(row)


@click.command()
@click.option(
    "--base-file",
    help="Clickhouse queries results from base version.",
    required=True,
)
@click.option(
    "--upgrade-file",
    help="Clickhouse queries results from upgrade version.",
    required=True,
)
@click.option(
    "--table",
    help="Clickhouse queries results from upgrade version.",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
def query_comparer(
    *,
    base_file: str,
    upgrade_file: str,
    table: str,
    log_level: Optional[str] = None,
) -> None:
    """
    compare results
    """
    setup_logging(log_level)
    setup_sentry()

    base = open(base_file)
    upgrade = open(upgrade_file)

    base_reader = csv.reader(base)
    upgrade_reader = csv.reader(upgrade)

    def query_measurement(row) -> QueryMeasurement:
        return QueryMeasurement(
            query_id=row[0],
            query_duration_ms=int(row[1]),
            result_rows=int(row[2]),
            result_bytes=int(row[3]),
            read_rows=int(row[4]),
            read_bytes=int(row[5]),
        )

    mismatches = []
    total_rows = 0
    for v1_row, v2_row in itertools.zip_longest(base_reader, upgrade_reader):
        if v1_row[0] == "query_id":
            # csv header row
            continue

        assert v1_row[0] == v2_row[0], "Invalid query_ids: ids must match"
        total_rows += 1

        mismatches_exist = any(
            [1 for i in range(1, len(v1_row)) if v1_row[i] != v2_row[i]]
        )
        if not mismatches_exist:
            continue

        v1_data = query_measurement(v1_row)
        v2_data = query_measurement(v2_row)

        mismatches.append(_create_mismatch(v1_data, v2_data))

    _send_slack_report(total_rows, mismatches, table)


def _create_mismatch(v1_data, v2_data):
    mismatch = {}
    for m in MEASUREMENTS:
        v1_value = v1_data.__getattribute__(m)
        v2_value = v2_data.__getattribute__(m)

        delta = v2_value - v1_value
        mismatched_values = MismatchedValues(
            v1_value,
            v2_value,
            delta,
        )
        mismatch[m] = mismatched_values
    return QueryMismatch(
        query_id=v1_data.query_id,
        query_duration_ms=mismatch["query_duration_ms"],
        result_rows=mismatch["result_rows"],
        result_bytes=mismatch["result_bytes"],
        read_rows=mismatch["read_rows"],
        read_bytes=mismatch["read_bytes"],
    )


def _format_slack_overview(total_rows, mismatches, table) -> Any:
    queries = total_rows
    num_mismatches = len(mismatches)
    per_mismatches = len(mismatches) / total_rows

    overview_text = f"*Overview: `{table}`*\n Total Queries: `{queries}`\n Total Mismatches: `{num_mismatches}`\n% Mismatch: `{per_mismatches}`"
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Querylog Comparison `21.8` v `22.8`*\n",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": overview_text,
                },
            },
        ]
    }


def _send_slack_report(total_rows, mismatches, table) -> None:
    slack_client = SlackClient(
        channel_id=settings.SNUBA_SLACK_CHANNEL_ID, token=settings.SLACK_API_TOKEN
    )

    p_header_row = [
        "query_id",
        "ms_1",
        "ms_2",
        "rows1",
        "rows2",
        "bytes1",
        "bytes2",
        "duration_delta",
        "read_rows_delta",
        "read_bytes_delta",
    ]
    p_results = [m.performance_mismatches() for m in mismatches]

    d_header_row = [
        "query_id",
        "rows1",
        "rows2",
        "bytes1",
        "bytes2",
        "result_rows_delta",
        "result_bytes_delta",
    ]
    d_results = [m.data_mismatches() for m in mismatches]

    p_filename = f"perf_{table}.csv"
    d_filename = f"data_{table}.csv"

    slack_client.post_message(
        message=_format_slack_overview(total_rows, mismatches, table)
    )

    for results, header_row, filename in [
        (p_results, p_header_row, p_filename),
        (d_results, d_header_row, d_filename),
    ]:
        write_querylog_comparison_results_to_csv(
            results, f"/tmp/{filename}", header_row
        )
        slack_client.post_file(
            file_name=filename,
            file_path=f"/tmp/{filename}",
            file_type="csv",
            initial_comment=f"Querylog Result Report: {filename}",
        )
