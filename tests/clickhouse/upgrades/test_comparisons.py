from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

from snuba.clickhouse.upgrades.comparisons import (
    FileFormat,
    FileManager,
    QueryInfoResult,
)
from snuba.utils.gcs import Blobs


class MockUploader(Mock):
    def __init__(self, bucket: str) -> None:
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
        return Blobs([], [])

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
