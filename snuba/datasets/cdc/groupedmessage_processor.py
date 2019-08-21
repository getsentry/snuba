from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from dataclasses import dataclass
from dateutil.parser import parse as dateutil_parse
from snuba.datasets.cdc.cdcprocessors import CdcProcessor, CdcMessageRow
from snuba.writer import WriterTableRow


@dataclass(frozen=True)
class GroupMessageRecord:
    status: int
    last_seen: datetime
    first_seen: datetime
    active_at: Optional[datetime] = None
    first_release_id: Optional[int] = None


@dataclass(frozen=True)
class GroupedMessageRow(CdcMessageRow):
    offset: Optional[int]
    id: int
    record_deleted: bool
    record_content: Optional[GroupMessageRecord]

    @classmethod
    def from_wal(cls,
        offset: int,
        columnnames: Sequence[Any],
        columnvalues: Sequence[Any],
    ) -> GroupedMessageRow:
        raw_data = dict(zip(columnnames, columnvalues))
        return GroupedMessageRow(
            offset=offset,
            id=raw_data['id'],
            record_deleted=False,
            record_content=GroupMessageRecord(
                status=raw_data['status'],
                last_seen=dateutil_parse(raw_data['last_seen']),
                first_seen=dateutil_parse(raw_data['first_seen']),
                active_at=dateutil_parse(raw_data['active_at']),
                first_release_id=raw_data['first_release_id'],
            )
        )

    @classmethod
    def from_bulk(cls,
        row: Mapping[str, Any],
    ) -> GroupedMessageRow:
        return GroupedMessageRow(
            offset=None,
            id=int(row['id']),
            record_deleted=False,
            record_content=GroupMessageRecord(
                status=int(row['status']),
                last_seen=dateutil_parse(row['last_seen']),
                first_seen=dateutil_parse(row['first_seen']),
                active_at=dateutil_parse(row['active_at']),
                first_release_id=int(row['first_release_id']) if row['first_release_id'] else None,
            )
        )

    def to_clickhouse(self) -> WriterTableRow:
        deleted = self.record_content is None
        record = self.record_content
        return {
            'offset': self.offset if self.offset is not None else 0,
            'id': self.id,
            'record_deleted': 1 if self.record_deleted else 0,
            'status': None if deleted else record.status,
            'last_seen': None if deleted else record.last_seen,
            'first_seen': None if deleted else record.first_seen,
            'active_at': None if deleted else record.active_at,
            'first_release_id': None if deleted else record.first_release_id,
        }


class GroupedMessageProcessor(CdcProcessor):

    def __init__(self, postgres_table):
        super(GroupedMessageProcessor, self).__init__(
            pg_table=postgres_table,
            message_row_class=GroupedMessageRow,
        )

    def _process_delete(self, offset, key) -> Optional[WriterTableRow]:
        key_names = key['keynames']
        key_values = key['keyvalues']
        id = key_values[key_names.index('id')]
        return GroupedMessageRow(
            offset=offset,
            id=id,
            record_deleted=True,
            record_content=None
        ).to_clickhouse()
