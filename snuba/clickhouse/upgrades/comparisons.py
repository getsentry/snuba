import csv
from datetime import datetime
from typing import NamedTuple, Sequence, Union

import structlog

from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)


class QueryInfoResult(NamedTuple):
    query_str: str
    query_id: str


class QueryMeasurementResult(NamedTuple):
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int
    read_rows: int
    read_bytes: int


Results = Union[QueryInfoResult, QueryMeasurementResult]

DIRECTORY_RESULT_TYPES = {
    "queries": QueryInfoResult,
    "results": QueryMeasurementResult,
}


class FileFormat(NamedTuple):
    directory: str
    date: datetime
    table: str
    hour: int


def type_for_directory(directory) -> Results:
    if directory.startswith("results"):
        # remove the versioning e.g. results-22-8
        directory = "results"
    return DIRECTORY_RESULT_TYPES[directory]


class FileManager:
    def __init__(self, uploader: GCSUploader) -> None:
        self.uploader = uploader

    def _result_type(self, filename) -> Results:
        directory = filename.split("_", 1)[0]
        return type_for_directory(directory)

    def _format_filename(self, file_format: FileFormat) -> str:
        # Example: {dir}_2024_01_16_errors_local_1 - first hour
        #          {dir}_2024_01_16_errors_local_2 - second hour
        directory, date, table, hour = file_format
        day = datetime.strftime(date, "%Y_%m_%d")
        return f"{directory}_{day}_{table}_{hour}.csv"

    def _format_blob_name(self, file_format: FileFormat) -> str:
        # Example: {dir}/2024_01_16/errors_local_1 - first hour
        #          {dir}/2024_01_16/errors_local_2- second hour
        directory, date, table, hour = file_format
        day = datetime.strftime(date, "%Y_%m_%d")
        return f"{directory}/{day}/{table}_{hour}.csv"

    def _full_path(self, filename: str) -> str:
        return f"/tmp/{filename}"

    def _save_to_csv(
        self, filename: str, results: Sequence[Results], header_row: bool = False
    ) -> None:
        result_type = self._result_type(filename)
        with open(self._full_path(filename), mode="w") as file:
            writer = csv.writer(file)
            if header_row:
                fields = list(result_type._fields)
                writer.writerow(fields)
            for row in results:
                writer.writerow(row)

        logger.info(f"File {self._full_path(filename)} saved")

    def _download_from_csv(self, filename) -> Results:
        result_type = self._result_type(filename)
        results: Sequence[Results] = []
        with open(self._full_path(filename), mode="r") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                results.append(result_type(*row))
        return results

    def _save_to_gcs(self, filename: str, blob_name: str) -> None:
        self.uploader.upload_file(self._full_path(filename), blob_name)

    def _download_from_gcs(self, blob_name: str, filename: str) -> None:
        self.uploader.download_file(blob_name, self._full_path(filename))

    def filename_from_blob_name(self, blob_name: str) -> str:
        return blob_name.replace("/", "_")

    def parse_blob_name(self, blob_name) -> FileFormat:
        directory, date, _ = blob_name.split("/")
        table, hour = blob_name.split("/")[-1].rsplit("_", 1)
        hour = hour.replace(".csv", "")
        return FileFormat(directory, datetime.strptime(date, "%Y_%m_%d"), table, hour)

    def save(self, file_format: FileFormat, results: Sequence[Results]) -> None:
        """
        First save the results to local csv file,
        then upload the file to gcs bucket.
        """
        filename = self._format_filename(file_format)
        self._save_to_csv(filename, results)

        blob_name = self._format_blob_name(file_format)
        self._save_to_gcs(filename, blob_name)

    def download(self, blob_name: str) -> Sequence[Results]:
        filename = self.filename_from_blob_name(blob_name)
        self._download_from_gcs(blob_name, filename)
        return self._download_from_csv(filename)
