from __future__ import annotations

import csv
import json
import logging
import os.path
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Iterable, Iterator, NewType, Sequence

import jsonschema

from snuba import settings
from snuba.snapshots import (
    BulkLoadSource,
    SnapshotDescriptor,
    SnapshotTableRow,
    TableConfig,
)

Xid = NewType("Xid", int)

SNAPSHOT_METADATA_SCHEMA = {
    "type": "object",
    "properties": {
        "snapshot_id": {"type": "string"},
        "product": {"type": "string"},
        "transactions": {
            "type": "object",
            "properties": {
                "xmax": {"type": "number"},
                "xmin": {"type": "number"},
                "xip_list": {"type": "array", "items": {"type": "number"}},
            },
            "required": ["xmax", "xmin", "xip_list"],
        },
        "content": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "zip": {"type": "boolean"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "format": {
                                    "anyOf": [
                                        # Each object provides a different formatter with different parameters
                                        {
                                            "type": "object",
                                            "properties": {
                                                "type": {"const": "datetime"},
                                                "precision": {
                                                    "type": "string",
                                                    "enum": ["second"],
                                                },
                                            },
                                            "required": ["type", "format"],
                                        }
                                    ]
                                },
                            },
                            "required": ["name"],
                        },
                    },
                },
                "required": ["table"],
            },
        },
        "start_timestamp": {"type": "number"},
    },
    "required": [
        "snapshot_id",
        "product",
        "transactions",
        "content",
        "start_timestamp",
    ],
}


@dataclass(frozen=True)
class PostgresSnapshotDescriptor(SnapshotDescriptor):
    """
    Provides the metadata for the loaded snapshot.
    """

    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]


logger = logging.getLogger("snuba.postgres-snapshot")


class PostgresSnapshot(BulkLoadSource):
    """
    TODO: Make this a library to be reused outside of Snuba when after this
    is validated in production.

    Represents a snapshot from a Postgres instance and dumped into
    a set of files (one per table).

    This class know how to read and validate the dump.
    """

    def __init__(self, path: str, descriptor: PostgresSnapshotDescriptor) -> None:
        self.__path = path
        self.__descriptor = descriptor

    @classmethod
    def load(cls, product: str, path: str) -> PostgresSnapshot:
        meta_file_name = os.path.join(path, "metadata.json")
        with open(meta_file_name, "r") as meta_file:
            json_desc = json.load(meta_file)
            jsonschema.validate(
                json_desc,
                SNAPSHOT_METADATA_SCHEMA,
            )

            if json_desc["product"] != product:
                raise ValueError(
                    "Invalid product in Postgres snapshot %s. Expected %s"
                    % (json_desc["product"], product)
                )

            desc_content = [
                TableConfig.from_dict(table) for table in json_desc["content"]
            ]

            descriptor = PostgresSnapshotDescriptor(
                id=json_desc["snapshot_id"],
                xmin=json_desc["transactions"]["xmin"],
                xmax=json_desc["transactions"]["xmax"],
                xip_list=json_desc["transactions"]["xip_list"],
                tables=desc_content,
            )

            logger.debug("Loading snapshot %r ", descriptor)

            return PostgresSnapshot(path, descriptor)

    def get_descriptor(self) -> PostgresSnapshotDescriptor:
        return self.__descriptor

    def __get_table_path(self, table_name: str) -> str:
        table_desc = self.__descriptor.get_table(table_name)
        table_file_name = (
            f"{table_name}.csv" if not table_desc.zip else f"{table_name}.csv.gz"
        )
        return os.path.join(self.__path, "tables", table_file_name)

    def get_table_file_size(self, table_name: str) -> int:
        path = self.__get_table_path(table_name)
        return os.stat(path).st_size

    @contextmanager
    def get_parsed_table_file(
        self,
        table: str,
    ) -> Generator[Iterator[SnapshotTableRow], None, None]:
        table_desc = self.__descriptor.get_table(table)
        assert not table_desc.zip, "Cannot parse a gzip table file on the fly"

        table_path = self.__get_table_path(table)
        try:
            with open(table_path, "r") as table_file:
                csv_file = csv.DictReader(table_file)
                columns = csv_file.fieldnames

                descriptor_columns = self.__descriptor.get_table(table).columns
                assert (
                    descriptor_columns is not None
                ), "Cannot import a snapshot that does not provide a columns list"
                expected_columns = [c.name for c in descriptor_columns]
                if expected_columns:
                    expected_set = set(expected_columns)
                    assert isinstance(columns, Iterable)
                    existing_set = set(columns)
                    if not expected_set <= existing_set:
                        raise ValueError(
                            "The table %s is missing columns %r "
                            % (
                                table,
                                expected_set - existing_set,
                            )
                        )

                    if len(existing_set) != len(expected_set):
                        logger.warning(
                            "The table %s contains more columns than expected %r",
                            table,
                            existing_set - expected_set,
                        )
                else:
                    logger.info(
                        "Won't pre-validate snapshot columns. There is nothing in the descriptor"
                    )

                yield csv_file

        except FileNotFoundError:
            raise ValueError(
                "The snapshot does not contain the requested table %s" % table,
            )

    @contextmanager
    def get_preprocessed_table_file(
        self, table: str
    ) -> Generator[Iterator[bytes], None, None]:
        table_path = self.__get_table_path(table)

        try:
            with open(table_path, "rb") as table_file:

                def chunks_provider() -> Iterator[bytes]:
                    for chunk in iter(
                        lambda: table_file.read(settings.BULK_BINARY_LOAD_CHUNK), b""
                    ):
                        yield chunk

                yield chunks_provider()

        except FileNotFoundError:
            raise ValueError(
                "The snapshot does not contain the requested table %s" % table,
            )
