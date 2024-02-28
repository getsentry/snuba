import itertools
from typing import Any, Optional, Sequence, Tuple

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


@click.command()
@click.option(
    "--gcs-bucket",
    help="Name of gcs bucket to save query files to",
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
        logger.info("Not enough results to compare.")
        return

    def get_matched_pairs() -> Sequence[Tuple[str, str]]:
        matches = []
        v1_prefix, v2_prefix = result_prefixes
        for v1_name in blob_getter.get_all_names(prefix=v1_prefix):
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
            # need the filename for the slack report
            filename = file_manager.format_filename(compared_file_format)
            _send_slack_report(
                compare_type, total_rows, mismatches, result_file_format.table, filename
            )

        if compare_type == "perf":
            save_comparisons(compare_type, perf_mismatches)
        elif compare_type == "data":
            save_comparisons(compare_type, data_mismatches)
        else:
            # no compare type == run both reports
            save_comparisons("perf", perf_mismatches)
            save_comparisons("data", data_mismatches)


PERF_THRESHOLDS = {
    "query_duration_ms": 0,
    "read_rows": 0,
    "read_bytes": 0,
}

DATA_THRESHOLDS = {
    "result_rows": 0,
    "result_bytes": 0,
}


def create_perf_mismatch(
    v1_data: QueryMeasurementResult, v2_data: QueryMeasurementResult
) -> Optional[PerfMismatchResult]:
    d_ms_1 = v1_data.query_duration_ms
    d_ms_2 = v2_data.query_duration_ms
    d_ms_delta = d_ms_2 - d_ms_1

    rd_r_1 = v1_data.read_rows
    rd_r_2 = v2_data.read_rows
    rd_r_delta = rd_r_2 - rd_r_1

    rd_b_1 = v1_data.read_bytes
    rd_b_2 = v2_data.read_bytes
    rd_b_delta = rd_b_2 - rd_b_1

    if (
        (d_ms_delta > PERF_THRESHOLDS["query_duration_ms"])
        or rd_r_delta > PERF_THRESHOLDS["read_rows"]
        or rd_b_delta > PERF_THRESHOLDS["read_bytes"]
    ):
        # mismatch!
        return PerfMismatchResult(
            v1_data.query_id,
            d_ms_1=d_ms_1,
            d_ms_2=d_ms_2,
            rd_r_1=rd_r_1,
            rd_r_2=rd_r_2,
            rd_b_1=rd_b_1,
            rd_b_2=rd_b_2,
            d_ms_delta=d_ms_delta,
            rd_r_delta=rd_r_delta,
            rd_b_delta=rd_b_delta,
        )
    return None


def create_data_mismatch(
    v1_data: QueryMeasurementResult, v2_data: QueryMeasurementResult
) -> Optional[DataMismatchResult]:
    rt_r_1 = v1_data.result_rows
    rt_r_2 = v2_data.result_rows
    rt_r_delta = rt_r_2 - rt_r_1

    rt_b_1 = v1_data.result_bytes
    rt_b_2 = v2_data.result_bytes
    rt_b_delta = rt_b_2 - rt_b_1

    if (rt_r_delta > DATA_THRESHOLDS["result_rows"]) or rt_b_delta > DATA_THRESHOLDS[
        "result_bytes"
    ]:
        # mismatch!
        return DataMismatchResult(
            v1_data.query_id,
            rt_r_1=rt_r_1,
            rt_r_2=rt_r_2,
            rt_b_1=rt_b_1,
            rt_b_2=rt_b_2,
            rt_r_delta=rt_r_delta,
            rt_b_delta=rt_b_delta,
        )
    return None


def _format_slack_overview(
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
    compare_type: str,
    total_rows: int,
    mismatches: MismatchResultSet,
    table: str,
    filename: str,
) -> None:
    slack_client = SlackClient(
        channel_id=settings.SNUBA_SLACK_CHANNEL_ID, token=settings.SLACK_API_TOKEN
    )

    slack_client.post_message(
        message=_format_slack_overview(total_rows, mismatches, table, compare_type)
    )

    slack_client.post_file(
        file_name=filename,
        file_path=f"/tmp/{filename}",
        file_type="csv",
        initial_comment=f"report from: {filename}",
    )
