from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from snuba.cli.query_comparer import (
    TableTotals,
    create_data_mismatch,
    create_perf_mismatch,
)
from snuba.clickhouse.upgrades.comparisons import (
    BlobGetter,
    FileFormat,
    FileManager,
    QueryInfoResult,
    QueryMeasurementResult,
)
from snuba.utils.gcs import Blobs


class MockUploader(Mock):
    def __init__(self, bucket: str) -> None:
        super().__init__()
        self.bucket = bucket

    def upload_file(
        self, source_file_name: str, destination_blob_name: Optional[str] = None
    ) -> None:
        pass

    def download_file(self, source_blob_name: str, destination_file_name: str) -> None:
        pass

    def list_blobs(
        self, prefix: Optional[str] = None, delimiter: Optional[str] = None
    ) -> Blobs:
        names = []
        prefixes = []
        if prefix == "queries/":
            names = (
                [
                    "queries/2024_02_15/meredith_test_22.csv",
                    "queries/2024_02_15/meredith_test_23.csv",
                    "queries/2024_02_16/meredith_test_1.csv",
                    "queries/2024_02_16/meredith_test_2.csv",
                ]
                if not delimiter
                else []
            )
            prefixes = (
                [
                    "queries/2024_02_15/",
                    "queries/2024_02_16/",
                ]
                if delimiter
                else []
            )

        if prefix == "queries/2024_02_16/":
            names = (
                [
                    "queries/2024_02_16/meredith_test_1.csv",
                    "queries/2024_02_16/meredith_test_2.csv",
                ]
                if not delimiter
                else []
            )

        if prefix == "results-21-8/":
            names = (
                [
                    "results-21-8/2024_02_15/meredith_test_22.csv",
                    "results-21-8/2024_02_15/meredith_test_23.csv",
                ]
                if not delimiter
                else []
            )
            prefixes = (
                [
                    "results-21-8/2024_02_15/",
                ]
                if delimiter
                else []
            )

        return Blobs(names, prefixes)

    def blob_exists(self, source_blob_name: str) -> bool:
        return True


TIMESTAMP = datetime.utcnow()


@patch("snuba.clickhouse.upgrades.comparisons.GCSUploader")
def test_file_manager(mock_uploader: Mock) -> None:
    """
    Save some results and then download those results
    to make sure you get back the same thing.
    """
    mock_uploader = MockUploader("my-bucket")
    format = FileFormat("queries", TIMESTAMP, "meredith_test", 1)
    results = [QueryInfoResult("select * from meredith_test", "xxxx-xxxx-xxxxx-xxxx")]

    fm = FileManager(mock_uploader)
    fm.save(file_format=format, results=results)
    blob_name = fm._format_blob_name(format)
    assert results == fm.download(blob_name)


@patch("snuba.clickhouse.upgrades.comparisons.GCSUploader")
def test_blob_getter(mock_uploader: Mock) -> None:
    """
    Test the BlobGetter, both getting the full names
    of the all the blobs, given a prefix, and also
    the blob names that differ between two prefixes.
    """
    mock_uploader = MockUploader("my-bucket")
    bg = BlobGetter(mock_uploader)

    invalid_blob_names = bg.get_all_names("blahhh/")
    assert len(invalid_blob_names) == 0

    q_prefix = "queries/"
    queries_blob_names = bg.get_all_names("queries/")
    assert len(queries_blob_names) == 4

    r_prefix = "results-21-8/"
    results_blob_names = bg.get_all_names("results-21-8/")
    assert len(results_blob_names) == 2

    blob_diffs = bg.get_name_diffs((q_prefix, r_prefix))
    assert blob_diffs == [
        "queries/2024_02_16/meredith_test_1.csv",
        "queries/2024_02_16/meredith_test_2.csv",
    ]


perf_mismatch_tests = [
    pytest.param(
        30,
        45,
        True,
        id="Slower duration - over threshold, over 10ms delta",
    ),
    pytest.param(
        30,
        35,
        False,
        id="Slower duration - over threshold, under 10ms delta ",
    ),
    pytest.param(
        30,
        32,
        False,
        id="Slower duration - under threshold, under 10ms delta ",
    ),
    pytest.param(
        30,
        20,
        False,
        id="Faster duration",
    ),
]


@pytest.mark.parametrize(
    "first_duration, second_duration, is_mismatch", perf_mismatch_tests
)
def test_perf_mismatch(
    first_duration: int, second_duration: int, is_mismatch: bool
) -> None:
    first_measurement = QueryMeasurementResult(
        query_id="xxxx-xxxx-xxxx-xxxx",
        query_duration_ms=first_duration,
        result_rows=0,
        result_bytes=0,
        read_rows=0,
        read_bytes=0,
    )
    second_measurement = QueryMeasurementResult(
        query_id="xxxx-xxxx-xxxx-xxxx",
        query_duration_ms=second_duration,
        result_rows=0,
        result_bytes=0,
        read_rows=0,
        read_bytes=0,
    )

    mismatch = create_perf_mismatch(first_measurement, second_measurement)
    if is_mismatch:
        assert mismatch
    else:
        assert mismatch is None


data_mismatch_tests = [
    pytest.param(
        0,
        0,
        12,
        16,
        True,
        id="Higher result_bytes",
    ),
    pytest.param(
        0,
        0,
        12,
        10,
        True,
        id="Lower result_bytes",
    ),
    pytest.param(
        10,
        12,
        0,
        0,
        True,
        id="Higher result_rows",
    ),
    pytest.param(
        10,
        8,
        0,
        0,
        True,
        id="Lower results_rows",
    ),
    pytest.param(
        10,
        10,
        12,
        12,
        False,
        id="Equal result rows & bytes",
    ),
]


@pytest.mark.parametrize(
    "first_result_rows, second_result_rows, first_result_bytes, second_result_bytes, is_mismatch",
    data_mismatch_tests,
)
def test_data_mismatch(
    first_result_rows: int,
    second_result_rows: int,
    first_result_bytes: int,
    second_result_bytes: int,
    is_mismatch: bool,
) -> None:
    first_measurement = QueryMeasurementResult(
        query_id="xxxx-xxxx-xxxx-xxxx",
        query_duration_ms=0,
        result_rows=first_result_rows,
        result_bytes=first_result_bytes,
        read_rows=0,
        read_bytes=0,
    )
    second_measurement = QueryMeasurementResult(
        query_id="xxxx-xxxx-xxxx-xxxx",
        query_duration_ms=0,
        result_rows=second_result_rows,
        result_bytes=second_result_bytes,
        read_rows=0,
        read_bytes=0,
    )

    mismatch = create_data_mismatch(first_measurement, second_measurement)
    if is_mismatch:
        assert mismatch
    else:
        assert mismatch is None


def test_table_totals() -> None:
    totals = TableTotals()
    totals.add(10, 5, 6)
    assert totals.total_queries == 10
    assert totals.total_data_mismatches == 5
    assert totals.total_perf_mismatches == 6

    totals.add(10, 5, 6)
    assert totals.total_queries == 20
    assert totals.total_data_mismatches == 10
    assert totals.total_perf_mismatches == 12
