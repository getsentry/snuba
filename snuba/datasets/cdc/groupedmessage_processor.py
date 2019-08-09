from datetime import datetime
from typing import Any, Mapping, Optional

from dataclasses import dataclass
from dateutil.parser import parse as dateutil_parse
from snuba.datasets.cdc.cdcprocessors import CdcProcessor


@dataclass(frozen=True)
class GroupedMessageRow:
    offset: int
    id: int
    record_deleted: bool
    status: Optional[int]
    last_seen: Optional[datetime]
    first_seen: Optional[datetime]
    active_at: Optional[datetime]
    first_release_id: Optional[int]

    @classmethod
    def from_wal(cls, offset, columnnames, columnvalues):
        raw_data = dict(zip(columnnames, columnvalues))
        return GroupedMessageRow(
            offset=offset,
            id=raw_data['id'],
            record_deleted=False,
            status=raw_data['status'],
            last_seen=dateutil_parse(raw_data['last_seen']),
            first_seen=dateutil_parse(raw_data['first_seen']),
            active_at=dateutil_parse(raw_data['active_at']),
            first_release_id=raw_data['first_release_id'],
        )

    @classmethod
    def from_bulk(cls, row):
        return GroupedMessageRow(
            offset=0,
            id=int(row['id']),
            record_deleted=False,
            status=int(row['status']),
            last_seen=dateutil_parse(row['last_seen']),
            first_seen=dateutil_parse(row['first_seen']),
            active_at=dateutil_parse(row['active_at']),
            first_release_id=int(row['first_release_id']) if row['first_release_id'] else None,
        )

    def to_clickhouse(self) -> Mapping[str, Any]:
        return {
            'offset': self.offset,
            'id': self.id,
            'record_deleted': 1 if self.record_deleted else 0,
            'status': self.status,
            'last_seen': self.last_seen,
            'first_seen': self.first_seen,
            'active_at': self.active_at,
            'first_release_id': self.first_release_id,
        }


class GroupedMessageProcessor(CdcProcessor):

    def __init__(self, postgres_table):
        super(GroupedMessageProcessor, self).__init__(
            pg_table=postgres_table,
        )

    def _process_insert(self, offset, columnnames, columnvalues):
        return GroupedMessageRow.from_wal(
            offset,
            columnnames,
            columnvalues
        ).to_clickhouse()

    def _process_delete(self, offset, key):
        key_names = key['keynames']
        key_values = key['keyvalues']
        id = key_values[key_names.index('id')]
        return GroupedMessageRow(
            offset=offset,
            id=id,
            record_deleted=True,
        ).to_clickhouse()

    def _process_update(self, offset, key, columnnames, columnvalues):
        new_id = columnvalues[columnnames.index('id')]
        key_names = key['keynames']
        key_values = key['keyvalues']
        old_id = key_values[key_names.index('id')]
        # We cannot support a change in the identity of the record
        # clickhouse will use the identity column to find rows to merge.
        # if we change it, merging won't work.
        assert old_id == new_id, 'Changing Primary Key is not supported.'
        return GroupedMessageRow.from_wal(
            offset,
            columnnames,
            columnvalues
        ).to_clickhouse()
