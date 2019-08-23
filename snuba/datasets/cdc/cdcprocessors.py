from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional, Sequence, Type

from snuba.processor import MessageProcessor
from snuba.writer import WriterTableRow

KAFKA_ONLY_PARTITION = 0  # CDC only works with single partition topics. So partition must be 0


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


class CdcProcessor(MessageProcessor):

    def __init__(self, pg_table: str, message_row_class: Type[CdcMessageRow]):
        self.pg_table = pg_table
        self._message_row_class = message_row_class

    def _process_begin(self, offset: int):
        pass

    def _process_commit(self, offset: int):
        pass

    def _process_insert(self,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> Optional[WriterTableRow]:
        return self._message_row_class.from_wal(
            offset,
            columnnames,
            columnvalues
        ).to_clickhouse()

    def _process_update(self,
        offset: int,
        key: Mapping[str, Any],
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> Optional[WriterTableRow]:
        old_key = dict(zip(key['keynames'], key['keyvalues']))
        new_key = {
            key: columnvalues[columnnames.index(key)]
            for key
            in key['keynames']
        }
        # We cannot support a change in the identity of the record
        # clickhouse will use the identity column to find rows to merge.
        # if we change it, merging won't work.
        assert old_key == new_key, 'Changing Primary Key is not supported.'
        return self._message_row_class.from_wal(
            offset,
            columnnames,
            columnvalues
        ).to_clickhouse()

    def _process_delete(self,
        offset: int,
        key: Mapping[str, Any],
    ) -> Optional[WriterTableRow]:
        pass

    def process_message(self, value, metadata):
        assert isinstance(value, dict)

        partition = metadata.partition
        assert partition == KAFKA_ONLY_PARTITION, 'CDC can only work with single partition topics for consistency'

        offset = metadata.offset
        event = value['event']
        if event == 'begin':
            message = self._process_begin(offset)
        elif event == 'commit':
            message = self._process_commit(offset)
        elif event == 'change':
            table_name = value['table']
            if table_name != self.pg_table:
                return None

            operation = value['kind']
            if operation == 'insert':
                message = self._process_insert(
                    offset, value['columnnames'], value['columnvalues'])
            elif operation == 'update':
                message = self._process_update(
                    offset, value['oldkeys'], value['columnnames'], value['columnvalues'])
            elif operation == 'delete':
                message = self._process_delete(offset, value['oldkeys'])
            else:
                raise ValueError("Invalid value for operation in replication log: %s" % value['kind'])
        else:
            raise ValueError("Invalid value for event in replication log: %s" % value['event'])

        if message is None:
            return None

        return (self.INSERT, message)
