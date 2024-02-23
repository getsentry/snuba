import csv
import itertools
from dataclasses import dataclass
from typing import Any, NamedTuple, Optional, Sequence, Union

import click
import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.clickhouse.upgrades.comparisons import (
    BlobGetter,
    FileManager,
    QueryMeasurementResult,
)
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)


@dataclass(frozen=True)
class MismatchedValues:
    base_value: int
    new_value: int
    delta: int


class PerformanceMismatchResult(NamedTuple):
    query_id: str
    duration_ms_base: int
    duration_ms_new: int
    read_rows_base: int
    read_rows_new: int
    read_bytes_base: int
    read_bytes_new: int
    duration_ms_delta: int
    read_rows_delta: int
    read_bytes_delta: int


class DataMismatchResult(NamedTuple):
    query_id: str
    result_rows_base: int
    result_rows_new: int
    result_bytes_base: int
    result_bytes_new: int
    result_rows_delta: int
    result_bytes_delta: int


@dataclass
class QueryMismatch:
    query_id: str
    query_duration_ms: MismatchedValues
    result_rows: MismatchedValues
    result_bytes: MismatchedValues
    read_rows: MismatchedValues
    read_bytes: MismatchedValues

    def performance_mismatches(self) -> PerformanceMismatchResult:
        return PerformanceMismatchResult(
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

    def data_mismatches(self) -> DataMismatchResult:
        return DataMismatchResult(
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
    results: Union[Sequence[DataMismatchResult], Sequence[PerformanceMismatchResult]],
    filename: str,
    header_row: Sequence[str],
) -> None:
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(header_row)
        for row in results:
            writer.writerow(row)


@click.command()
@click.option(
    "--gcs-bucket",
    help="Name of gcs bucket to save query files to",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
def query_comparer(
    *,
    gcs_bucket: str,
    log_level: Optional[str] = None,
) -> None:
    """
    compare results
    """
    setup_logging(log_level)
    setup_sentry()

    # check the result versions
    # get the difference between the
    uploader = GCSUploader(gcs_bucket)
    file_manager = FileManager(uploader)
    blob_getter = BlobGetter(uploader)

    blob_prefixes = blob_getter._get_sub_dir_prefixes(prefix="")
    v1, v2 = sorted(
        [prefix for prefix in blob_prefixes if prefix.startswith("results")]
    )

    all_names = blob_getter.get_all_names(prefix="results")
    unmatched_names = blob_getter.get_name_diffs((v1, v2)) + blob_getter.get_name_diffs(
        (v2, v1)
    )

    def get_matched_pairs():
        matches = []
        v1_matches = sorted([name for name in all_names if name not in unmatched_names])
        midpoint = int(len(v1_matches) / 2)
        for i in range(0, midpoint):
            v1_match = v1_matches[i]
            v2_match = v1_match.replace(v1, v2)
            matches.append((v1_match, v2_match))
        return matches

    matched_pairs = get_matched_pairs()
    for (v1_blob, v2_blob) in matched_pairs:
        file_format = file_manager.parse_blob_name(v1_blob)
        v1_results = file_manager.download(v1_blob)
        v2_results = file_manager.download(v2_blob)

        mismatches = []
        total_rows = 0
        for v1_result, v2_result in itertools.zip_longest(v1_results, v2_results):
            assert (
                v1_result.query_id == v2_result.query_id
            ), "Invalid query_ids: ids must match"
            total_rows += 1
            if v1_result == v2_result:
                continue

            mismatches.append(_create_mismatch(v1_result, v2_result))

        _send_slack_report(total_rows, mismatches, file_format.table)


def _create_mismatch(
    v1_data: QueryMeasurementResult, v2_data: QueryMeasurementResult
) -> QueryMismatch:
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


def _format_slack_overview(
    total_rows: int, mismatches: Sequence[QueryMismatch], table: str
) -> Any:
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


def _send_slack_report(
    total_rows: int, mismatches: Sequence[QueryMismatch], table: str
) -> None:
    slack_client = SlackClient(
        channel_id=settings.SNUBA_SLACK_CHANNEL_ID, token=settings.SLACK_API_TOKEN
    )

    slack_client.post_message(
        message=_format_slack_overview(total_rows, mismatches, table)
    )

    if not mismatches:
        return

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
    p_results: Sequence[PerformanceMismatchResult] = [
        m.performance_mismatches() for m in mismatches
    ]

    d_header_row = [
        "query_id",
        "rows1",
        "rows2",
        "bytes1",
        "bytes2",
        "result_rows_delta",
        "result_bytes_delta",
    ]
    d_results: Sequence[DataMismatchResult] = [m.data_mismatches() for m in mismatches]

    p_filename = f"perf_{table}.csv"
    d_filename = f"data_{table}.csv"

    for results, header_row, filename in [
        (p_results, p_header_row, p_filename),
        (d_results, d_header_row, d_filename),
    ]:
        assert isinstance(results, (PerformanceMismatchResult, DataMismatchResult))
        write_querylog_comparison_results_to_csv(
            results, f"/tmp/{filename}", header_row
        )
        slack_client.post_file(
            file_name=filename,
            file_path=f"/tmp/{filename}",
            file_type="csv",
            initial_comment=f"Querylog Result Report: {filename}",
        )
