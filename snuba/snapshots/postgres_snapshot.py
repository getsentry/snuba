from __future__ import annotations

import jsonschema  # type: ignore
import json
import logging

from contextlib import contextmanager
from dataclasses import dataclass
from os import path as os_path
from typing import NewType, Generator, IO, Sequence

from snuba.snapshots import SnapshotDescriptor, SnapshotId, TableConfig
from snuba.snapshots import BulkLoadSource

Xid = NewType("Xid", int)


@dataclass(frozen=True)
class PostgresSnapshotDescriptor(SnapshotDescriptor):
    """
    Provides the metadata for the loaded snapshot.
    """
    id: SnapshotId
    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]
    tables: Sequence[TableConfig]


logger = logging.getLogger('postgres-snapshot')


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
    def load(cls, path: str, product: str) -> PostgresSnapshot:
        meta_file_name = os_path.join(path, "metadata.json")
        with open(meta_file_name, "r") as meta_file:
            json_desc = json.load(meta_file)
            jsonschema.validate(
                json_desc,
                {
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
                },
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
        table_path = os_path.join(self.__path, "tables/%s.csv" % table)
        with open(table_path, "r") as table_file:
            yield table_file
