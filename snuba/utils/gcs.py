import os
from typing import NamedTuple, Optional, Sequence

import structlog
from google.cloud.storage.client import Client  # type: ignore

logger = structlog.get_logger().bind(module=__name__)


class Blobs(NamedTuple):
    names: Sequence[str]
    prefixes: Sequence[str]


class GCSUploader:
    """
    Generic class to upload and download files from Google Cloud Storage.
    The credentials are read from the environment variable GOOGLE_APPLICATION_CREDENTIALS.
    GOOGLE_APPLICATION_CREDENTIALS must point to a JSON file containing the credentials.
    The google.cloud.storage.Client will automatically read the credentials from this file.
    We do not need to specify the credentials explicitly.
    """

    def __init__(self, bucket_name: str):
        assert (
            os.environ.get("GOOGLE_CLOUD_PROJECT") is not None
        ), "GOOGLE_CLOUD_PROJECT environment variable must be set."
        self.project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        self.bucket_name = bucket_name
        self.storage_client = Client(project=self.project_id)

    def upload_file(
        self, source_file_name: str, destination_blob_name: Optional[str] = None
    ) -> None:
        """
        Upload a file to the bucket. If no destination_blob_name is specified, the source file name is used.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        if destination_blob_name is None:
            destination_blob_name = os.path.basename(source_file_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        logger.info(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def download_file(self, source_blob_name: str, destination_file_name: str) -> None:
        """
        Downloads a file from the bucket to the local filesystem.
        The file is downloaded to destination_file_name.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)

        logger.info(f"File {source_blob_name} downloaded to {destination_file_name}.")

    def list_blobs(
        self, prefix: Optional[str] = None, delimiter: Optional[str] = None
    ) -> Blobs:
        """
        List blob names. If the prefix is provided, it will list blob names that exists
        under that prefix only. Delimiter is used if you want to get back prefixes. See
        https://cloud.google.com/storage/docs/listing-objects#list-objects for deats
        """
        blobs = self.storage_client.list_blobs(
            self.bucket_name, prefix=prefix, delimiter=delimiter
        )

        names = [blob.name for blob in blobs]
        prefixes = []
        if delimiter:
            prefixes = [prefix for prefix in blobs.prefixes]

        return Blobs(names, prefixes)

    def blob_exists(self, source_blob_name: str) -> bool:
        """
        Given a source blob name return whether or not it exists.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(source_blob_name)
        return True if blob.exists() else False  # satisfy mypy
