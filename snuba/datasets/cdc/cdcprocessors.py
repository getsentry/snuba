from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Mapping, Optional, Sequence, Type

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.registered_class import RegisteredClass
from snuba.writer import WriterTableRow

POSTGRES_DATE_FORMAT_WITH_NS = "%Y-%m-%d %H:%M:%S.%f%z"
POSTGRES_DATE_FORMAT_WITHOUT_NS = "%Y-%m-%d %H:%M:%S%z"

date_re = re.compile(
    "^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(\.\d{1,6})?(\+\d{2})"
)

date_with_nanosec = re.compile("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.")


def parse_postgres_datetime(date: str) -> datetime:
    # Postgres dates express the timezone in hours while strptime expects
    # the timezone to be expressed in HHmm, thus we need to add 00 as minutes.
    date = f"{date}00"
    if date_with_nanosec.match(date):
        return datetime.strptime(date, POSTGRES_DATE_FORMAT_WITH_NS)
    else:
        return datetime.strptime(date, POSTGRES_DATE_FORMAT_WITHOUT_NS)


def postgres_date_to_clickhouse(date: str) -> str:
    """
    Convert a postgres date time expressed as string.
    It returns a string that represent the date formatted for
    clickhouse.
    """
    match = date_re.match(date)
    assert match, f"Invalid date {date}"
    assert match[8] == "+00", f"Only support UTC timezone. date provided: {date}"
    return f"{match[1]}-{match[2]}-{match[3]} {match[4]}:{match[5]}:{match[6]}"


class CdcMessageRow(ABC):
    """
    Takes care of the data transformation from WAL to clickhouse and from
    bulk load to clickhouse. The goal is to keep all these transformation
    function in the same place because they ultimately have to be consistent
    with the Clickhouse schema.
    """

    @classmethod
    def from_wal(
        cls,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> CdcMessageRow:
        raise NotImplementedError

    @classmethod
    def from_bulk(
        cls,
        row: Mapping[str, Any],
    ) -> CdcMessageRow:
        raise NotImplementedError

    @abstractmethod
    def to_clickhouse(self) -> WriterTableRow:
        raise NotImplementedError


class CdcProcessor(DatasetMessageProcessor, metaclass=RegisteredClass):
    def __init__(self, pg_table: str, message_row_class: Type[CdcMessageRow]):
        self.pg_table = pg_table
        self._message_row_class = message_row_class

    def _process_begin(self, offset: int) -> Sequence[WriterTableRow]:
        return []

    def _process_commit(self, offset: int) -> Sequence[WriterTableRow]:
        return []

    def _process_insert(
        self,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> Sequence[WriterTableRow]:
        return [
            self._message_row_class.from_wal(
                offset, columnnames, columnvalues
            ).to_clickhouse()
        ]

    def _process_update(
        self,
        offset: int,
        key: Mapping[str, Any],
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> Sequence[WriterTableRow]:
        old_key = dict(zip(key["keynames"], key["keyvalues"]))
        new_key = {key: columnvalues[columnnames.index(key)] for key in key["keynames"]}

        ret: List[WriterTableRow] = []
        if old_key != new_key:
            ret.extend(self._process_delete(offset, key))

        ret.append(
            self._message_row_class.from_wal(
                offset, columnnames, columnvalues
            ).to_clickhouse()
        )
        return ret

    def _process_delete(
        self,
        offset: int,
        key: Mapping[str, Any],
    ) -> Sequence[WriterTableRow]:
        return []

    def process_message(
        self, value: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)

        offset = metadata.offset
        event = value["event"]
        timestamp: Optional[datetime] = None
        if event == "begin":
            messages = self._process_begin(offset)
        elif event == "commit":
            messages = self._process_commit(offset)
        elif event == "change":
            if "timestamp" in value:
                timestamp = parse_postgres_datetime(value["timestamp"])
            table_name = value["table"]
            if table_name != self.pg_table:
                return None

            operation = value["kind"]
            if operation == "insert":
                messages = self._process_insert(
                    offset, value["columnnames"], value["columnvalues"]
                )
            elif operation == "update":
                messages = self._process_update(
                    offset,
                    value["oldkeys"],
                    value["columnnames"],
                    value["columnvalues"],
                )
            elif operation == "delete":
                messages = self._process_delete(offset, value["oldkeys"])
            else:
                raise ValueError(
                    "Invalid value for operation in replication log: %s" % value["kind"]
                )
        else:
            raise ValueError(
                "Invalid value for event in replication log: %s" % value["event"]
            )

        if not messages:
            return None

        return InsertBatch(messages, timestamp)
