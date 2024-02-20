from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

from snuba.clickhouse.upgrades.comparisons import (
    BlobGetter,
    FileFormat,
    FileManager,
    QueryInfoResult,
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
