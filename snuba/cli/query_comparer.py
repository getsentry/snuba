import itertools
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional, Sequence, Tuple

import click
import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.clickhouse.upgrades.comparisons import (
    BlobGetter,
    DataMismatchResult,
    FileFormat,
    FileManager,
    PerfMismatchResult,
    QueryMeasurementResult,
)
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)

MismatchResultSet = Sequence[PerfMismatchResult | DataMismatchResult]


@dataclass
class TableTotals:
    total_queries: int = 0
    total_data_mismatches: int = 0
    total_perf_mismatches: int = 0

    def add(self, queries: int, data_mismatches: int, perf_mismatches: int) -> None:
        self.total_queries += queries
        self.total_data_mismatches += data_mismatches
        self.total_perf_mismatches += perf_mismatches


TOTALS_BY_TABLES: MutableMapping[str, TableTotals] = {}


@click.command()
@click.option(
    "--gcs-bucket",
    help="Name of gcs bucket to read query results from, and upload compared files to.",
    required=True,
)
@click.option(
    "--compare-type",
    help="Run comparisons on performance, data consistency or both (default).",
    type=click.Choice(["perf", "data"]),
)
@click.option(
    "--override",
    help="Option to override any previously re-run results",
    is_flag=True,
    default=False,
)
@click.option("--log-level", help="Logging level to use.")
def query_comparer(
    *,
    gcs_bucket: str,
    compare_type: str,
    override: bool,
    log_level: Optional[str] = None,
) -> None:
    """
    compare results
    """
    setup_logging(log_level)
    setup_sentry()

    uploader = GCSUploader(gcs_bucket)
    file_manager = FileManager(uploader)
    blob_getter = BlobGetter(uploader)

    blob_prefixes = blob_getter._get_sub_dir_prefixes(prefix="")
    result_prefixes = sorted([p for p in blob_prefixes if p.startswith("results")])

    if len(result_prefixes) != 2:
        raise Exception("Not enough results to compare.")

    def get_matched_pairs() -> Sequence[Tuple[str, str]]:
        """
        In order to compare results, we need results from both
        clickhouse versions: e.g. 21.8 & 22.8
        This function finds the matched blob pairs of results
        by looking at the blob names.

        returns a sequence of pairs:
        [
            ("results-21-8/2024_02_15/meredith_test_22.csv", "results-22-8/2024_02_15/meredith_test_22.csv"),
            ("results-21-8/2024_02_15/meredith_test_23.csv", "results-22-8/2024_02_15/meredith_test_23.csv"),
        ]
        """
        matches = []
        v1_prefix, v2_prefix = result_prefixes
        for v1_name in blob_getter.get_all_names(prefix=v1_prefix):
            # the blobs are named the same except for the prefix,
            # e.g. results-21-8/ vs results-22-8/
            v2_name = v1_name.replace(v1_prefix, v2_prefix)
            if uploader.blob_exists(v2_name):
                matches.append((v1_name, v2_name))
        return matches

    matched_pairs = get_matched_pairs()
    for (v1_blob, v2_blob) in matched_pairs:
        blob_suffix = v1_blob.split("/", 1)[-1]
        compared_perf_blob = f"compared-perf/{blob_suffix}"
        compared_data_blob = f"compared-data/{blob_suffix}"
        perf_blob_exists = uploader.blob_exists(compared_perf_blob)
        data_blob_exists = uploader.blob_exists(compared_data_blob)

        if perf_blob_exists and data_blob_exists and not override:
            continue

        if compare_type == "perf" and perf_blob_exists and not override:
            continue

        if compare_type == "data" and data_blob_exists and not override:
            continue

        result_file_format = file_manager.parse_blob_name(v1_blob)
        v1_results = file_manager.download(v1_blob)
        v2_results = file_manager.download(v2_blob)

        perf_mismatches = []
        data_mismatches = []
        total_rows = 0
        for v1_result, v2_result in itertools.zip_longest(v1_results, v2_results):
            assert (
                v1_result.query_id == v2_result.query_id
            ), f"Invalid query_ids: {v1_result.query_id} and {v2_result.query_id} must match"
            total_rows += 1
            if v1_result == v2_result:
                continue

            assert isinstance(v1_result, QueryMeasurementResult)
            assert isinstance(v2_result, QueryMeasurementResult)

            perf_mismatch = create_perf_mismatch(v1_result, v2_result)
            data_mismatch = create_data_mismatch(v1_result, v2_result)
            if perf_mismatch:
                perf_mismatches.append(perf_mismatch)
            if data_mismatch:
                data_mismatches.append(data_mismatch)

        def save_comparisons(compare_type: str, mismatches: MismatchResultSet) -> None:
            compared_file_format = FileFormat(
                f"compared-{compare_type}",
                result_file_format.date,
                result_file_format.table,
                result_file_format.hour,
            )
            file_manager.save(compared_file_format, mismatches, header_row=True)
            if mismatches:
                # need the filename for the slack report
                filename = file_manager.format_filename(compared_file_format)
                message = _format_file_slack_overview(
                    total_rows, mismatches, result_file_format.table, compare_type
                )
                _send_slack_report(message, filename)

        totals_by_table = TOTALS_BY_TABLES.get(result_file_format.table)
        if not totals_by_table:
            totals_by_table = TableTotals()
            TOTALS_BY_TABLES[result_file_format.table] = totals_by_table

        totals_by_table.add(
            queries=total_rows,
            data_mismatches=len(data_mismatches),
            perf_mismatches=len(perf_mismatches),
        )
        if compare_type == "perf":
            save_comparisons(compare_type, perf_mismatches)
        elif compare_type == "data":
            save_comparisons(compare_type, data_mismatches)
        else:
            # no compare type == run both reports
            save_comparisons("perf", perf_mismatches)
            save_comparisons("data", data_mismatches)

    for table in TOTALS_BY_TABLES.keys():
        message = _format_table_slack_overview(table, TOTALS_BY_TABLES[table])
        _send_slack_report(message)


PERF_PERCENT_THRESHOLD = 10

DATA_THRESHOLDS = {
    "result_rows": 0,
    "result_bytes": 0,
}


def create_perf_mismatch(
    v1_data: QueryMeasurementResult, v2_data: QueryMeasurementResult
) -> Optional[PerfMismatchResult]:
    duration_ms = v1_data.query_duration_ms
    duration_ms_new = v2_data.query_duration_ms
    duration_ms_delta = duration_ms_new - duration_ms

    read_rows = v1_data.read_rows
    read_rows_new = v2_data.read_rows
    read_rows_delta = read_rows_new - read_rows

    read_bytes = v1_data.read_bytes
    read_bytes_new = v2_data.read_bytes
    read_bytes_delta = read_bytes_new - read_bytes

    duration_percent = (duration_ms_delta / duration_ms) * 100
    # we use the delta as well as the percent threshold because for
    # super fast queries, 10ms diff is not that big of a deal but the
    # percentage would exceed the threshold.
    if duration_percent > PERF_PERCENT_THRESHOLD and (duration_ms_delta > 10):
        return PerfMismatchResult(
            v1_data.query_id,
            duration_ms=duration_ms,
            duration_ms_new=duration_ms_new,
            read_rows=read_rows,
            read_rows_new=read_rows_new,
            read_bytes=read_bytes,
            read_bytes_new=read_bytes_new,
            duration_ms_delta=duration_ms_delta,
            read_rows_delta=read_rows_delta,
            read_bytes_delta=read_bytes_delta,
        )
    return None


def create_data_mismatch(
    v1_data: QueryMeasurementResult, v2_data: QueryMeasurementResult
) -> Optional[DataMismatchResult]:
    result_rows = v1_data.result_rows
    result_rows_new = v2_data.result_rows
    result_rows_delta = result_rows_new - result_rows

    result_bytes = v1_data.result_bytes
    result_bytes_new = v2_data.result_bytes
    result_bytes_delta = result_bytes_new - result_bytes

    # higher or lower amount of rows/bytes is considered a mismatch
    if (abs(result_rows_delta) > DATA_THRESHOLDS["result_rows"]) or abs(
        result_bytes_delta
    ) > DATA_THRESHOLDS["result_bytes"]:
        return DataMismatchResult(
            v1_data.query_id,
            result_rows=result_rows,
            result_rows_new=result_rows_new,
            result_bytes=result_bytes,
            result_bytes_new=result_bytes_new,
            result_rows_delta=result_rows_delta,
            result_bytes_delta=result_bytes_delta,
        )
    return None


def _format_table_slack_overview(
    table: str,
    totals: TableTotals,
) -> Any:
    print("TALBLSB", totals)
    per_data_mismatches = (totals.total_data_mismatches / totals.total_queries) * 100
    per_perf_mismatches = (totals.total_perf_mismatches / totals.total_queries) * 100
    total_queries = f"Total Queries: `{totals.total_queries}`\n"
    total_data_mismatches = f"Total Data Mismatches: `{totals.total_data_mismatches}`\n"
    total_perf_mismatches = f"Total Perf Mismatches: `{totals.total_perf_mismatches}`\n"
    percent_data_mismatches = f"% Data Mismatch: `{per_data_mismatches}`\n"
    percent_perf_mismatches = f"% Perf Mismatch: `{per_perf_mismatches}`\n"
    overview_text = f"{total_queries} {total_data_mismatches} {percent_data_mismatches} {total_perf_mismatches} {percent_perf_mismatches}"
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Table Comparison Overview - {table}  `21.8` v `22.8`*\n",
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


def _format_file_slack_overview(
    total_rows: int,
    mismatches: MismatchResultSet,
    table: str,
    compare_type: str,
) -> Any:
    queries = total_rows
    num_mismatches = len(mismatches)
    per_mismatches = len(mismatches) / total_rows

    overview_text = f"*`{table}`*\n Total Queries: `{queries}`\n Total Mismatches: `{num_mismatches}`\n% Mismatch: `{per_mismatches}`"
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Querylog Comparison - {compare_type.upper()}  `21.8` v `22.8`*\n",
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
    message: Any,
    filename: Optional[str] = None,
) -> None:
    slack_client = SlackClient(
        channel_id=settings.SNUBA_SLACK_CHANNEL_ID, token=settings.SLACK_API_TOKEN
    )

    slack_client.post_message(message=message)

    if filename:
        slack_client.post_file(
            file_name=filename,
            file_path=f"/tmp/{filename}",
            file_type="csv",
            initial_comment=f"report from: {filename}",
        )
