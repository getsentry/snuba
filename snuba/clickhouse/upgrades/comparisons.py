import csv
import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, NamedTuple, Sequence, Tuple, Type, Union

import structlog

from snuba.utils.gcs import GCSUploader

logger = structlog.get_logger().bind(module=__name__)


@dataclass(frozen=True)
class MismatchedValues:
    base_value: int
    new_value: int
    delta: int


@dataclass
class PerfMismatchResult:
    query_id: str
    duration_ms: int
    duration_ms_new: int
    read_rows: int
    read_rows_new: int
    read_bytes: int
    read_bytes_new: int
    duration_ms_delta: int
    read_rows_delta: int
    read_bytes_delta: int


@dataclass
class DataMismatchResult:
    query_id: str
    result_rows: int
    result_rows_new: int
    result_bytes: int
    result_bytes_new: int
    result_rows_delta: int
    result_bytes_delta: int


@dataclass
class QueryInfoResult:
    query_str: str
    query_id: str


@dataclass
class QueryMeasurementResult:
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int
    read_rows: int
    read_bytes: int

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QueryMeasurementResult):
            return NotImplemented
        if self.query_duration_ms != other.query_duration_ms:
            return False
        if self.result_rows != other.result_rows:
            return False
        if self.result_bytes != other.result_bytes:
            return False
        if self.read_rows != other.read_rows:
            return False
        if self.read_bytes != other.read_bytes:
            return False
        return True


Results = Union[
    QueryInfoResult, QueryMeasurementResult, DataMismatchResult, PerfMismatchResult
]

DIRECTORY_RESULT_TYPES: Dict[str, Type[Results]] = {
    "queries": QueryInfoResult,
    "results": QueryMeasurementResult,
    "compared-data": DataMismatchResult,
    "compared-perf": PerfMismatchResult,
}


class FileFormat(NamedTuple):
    directory: str
    date: datetime
    table: str
    hour: int


def type_for_directory(directory: str) -> Type[Results]:
    if directory.startswith("results"):
        # remove the versioning e.g. results-22-8
        directory = "results"
    return DIRECTORY_RESULT_TYPES[directory]


class FileManager:
    def __init__(self, uploader: GCSUploader) -> None:
        self.uploader = uploader

    def _result_type(self, filename: str) -> Type[Results]:
        directory = filename.split("_", 1)[0]
        return type_for_directory(directory)

    def format_filename(self, file_format: FileFormat) -> str:
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
                fields = [f.name for f in dataclasses.fields(result_type)]  # mypy ig
                writer.writerow(fields)
            for row in results:
                writer.writerow(dataclasses.astuple(row))

        logger.debug(f"File {self._full_path(filename)} saved")

    def _download_from_csv(self, filename: str) -> Sequence[Results]:
        result_type = self._result_type(filename)
        results: List[Results] = []
        with open(self._full_path(filename), mode="r") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                result: Results
                if result_type == QueryInfoResult:
                    result = QueryInfoResult(*row)
                else:
                    result = QueryMeasurementResult(
                        row[0],
                        int(row[1]),
                        int(row[2]),
                        int(row[3]),
                        int(row[4]),
                        int(row[5]),
                    )
                results.append(result)
        return results

    def _save_to_gcs(self, filename: str, blob_name: str) -> None:
        self.uploader.upload_file(self._full_path(filename), blob_name)

    def _download_from_gcs(self, blob_name: str, filename: str) -> None:
        self.uploader.download_file(blob_name, self._full_path(filename))

    def filename_from_blob_name(self, blob_name: str) -> str:
        return blob_name.replace("/", "_")

    def parse_blob_name(self, blob_name: str) -> FileFormat:
        directory, date, _ = blob_name.split("/")
        table, hour = blob_name.split("/")[-1].rsplit("_", 1)
        hour = hour.replace(".csv", "")
        return FileFormat(
            directory, datetime.strptime(date, "%Y_%m_%d"), table, int(hour)
        )

    def save(
        self,
        file_format: FileFormat,
        results: Sequence[Results],
        header_row: bool = False,
    ) -> None:
        """
        First save the results to local csv file,
        then upload the file to gcs bucket.
        """
        filename = self.format_filename(file_format)
        self._save_to_csv(filename, results, header_row)

        blob_name = self._format_blob_name(file_format)
        self._save_to_gcs(filename, blob_name)

    def download(self, blob_name: str) -> Sequence[Results]:
        filename = self.filename_from_blob_name(blob_name)
        self._download_from_gcs(blob_name, filename)
        return self._download_from_csv(filename)


class BlobGetter:
    def __init__(self, uploader: GCSUploader) -> None:
        self.uploader = uploader

    def _get_sub_dir_prefixes(self, prefix: str) -> Sequence[str]:
        """
        Given a prefix 'queries/', this returns the following part of the
        prefix (if any).

        e.g Take the following blob names:
            * 'queries/2024_02_15/meredith_test_1.csv
            * 'queries/2024_02_16/meredith_test_1.csv

        calling _get_following_prefixes with 'queries/' would return
            * 2024_02_15/
            * 2024_02_16/
        """
        _, prefixes = self.uploader.list_blobs(prefix=prefix, delimiter="/")
        return [p.replace(prefix, "") for p in prefixes]

    def _get_sub_dir_diffs(self, prefixes: Tuple[str, str]) -> Sequence[str]:
        """
        Given two prefixes ('queries/', 'results-{version}/'), return the difference
        in the sub-directories from the first prefix. Basically, return the sub-
        directories that the first prefix has ('queries/') that are not present in
        the second prefix ('results-{version}') sub-directories.
        """
        p1, p2 = prefixes
        return list(
            set(self._get_sub_dir_prefixes(p1)).difference(
                set(self._get_sub_dir_prefixes(p2))
            )
        )

    def get_all_names(self, prefix: str) -> Sequence[str]:
        """
        Returns the full blob names for a given prefix, such as 'queries/'.
        e g. ['queries/2024_02_15/meredith_test_1.csv']
        """
        blob_names, _ = self.uploader.list_blobs(prefix=prefix, delimiter="")
        return blob_names

    def get_name_diffs(self, prefixes: Tuple[str, str]) -> Sequence[str]:
        """
        Get this difference of blobs between two prefixes. This is used to check whether
        query replay results need to be added ('queries/' vs. 'results-{version}/')
        or whether results need to be compared ('results-{verion}/' vs. 'compared/').

        The prefixes tuple order matters -> ('queries/', 'results-{version}/'), this will
        check which of the queries sub-directories are not present in the results.

        e.g Take the following blob names:
            * 'queries/2024_02_15/meredith_test_1.csv'
            * 'queries/2024_02_16/meredith_test_1.csv'

            * 'results-21-8/2024_02_15/meredith_test_1.csv'

            input: ('queries/', 'results-21-8/')
            output: [queries/2024_02_16/meredith_test_1.csv']
        """
        blob_diffs: List[str] = []
        primary_prefix = prefixes[0]

        sub_dir_diffs = self._get_sub_dir_diffs(prefixes)
        for sub_dir_prefix in sub_dir_diffs:
            full_prefix = f"{primary_prefix}{sub_dir_prefix}"
            blob_diffs += self.get_all_names(full_prefix)
        return blob_diffs
