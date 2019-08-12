from __future__ import annotations

import jsonschema  # type: ignore
import json
import logging
import os.path

from contextlib import contextmanager
from dataclasses import dataclass
from typing import NewType, Generator, IO, Sequence

from snuba.snapshots import SnapshotDescriptor, TableConfig
from snuba.snapshots import BulkLoadSource

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
                "xip_list": {
                    "type": "array",
                    "items": {"type": "number"},
                },
            },
            "required": ["xmax", "xmin", "xip_list"],
        },
        "content": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["table"],
            }
        },
        "start_timestamp": {
            "type": "number",
        },
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


logger = logging.getLogger('snuba.postgres-snapshot')


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
                raise ValueError("Invalid product in Postgres snapshot %s. Expected %s"
                    % (json_desc["product"], product))

            desc_content = [
                TableConfig(table["table"], table.get("columns")) for table in json_desc["content"]
            ]

            descriptor = PostgresSnapshotDescriptor(
                id=json_desc["snapshot_id"],
                xmin=json_desc["transactions"]["xmin"],
                xmax=json_desc["transactions"]["xmax"],
                xip_list=json_desc["transactions"]["xip_list"],
                tables=desc_content
            )

            logger.debug("Loading snapshot %r ", descriptor)

            return PostgresSnapshot(path, descriptor)

    def get_descriptor(self) -> PostgresSnapshotDescriptor:
        return self.__descriptor

    @contextmanager
    def get_table_file(self, table: str) -> Generator[IO[bytes], None, None]:
        table_path = os.path.join(self.__path, "tables", "%s.csv" % table)
        try:
            with open(table_path, "rb") as table_file:
                yield table_file
        except FileNotFoundError:
            raise ValueError(
                "The snapshot does not contain the requested table %s" % table,
            )
