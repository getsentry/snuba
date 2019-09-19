from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Union

from snuba.datasets.cdc.cdcprocessors import (
    CdcProcessor,
    CdcMessageRow,
    postgres_date_to_clickhouse,
    parse_postgres_datetime,
)
from snuba.writer import WriterTableRow


@dataclass(frozen=True)
class GroupAssigneeRecord:
    project_id: int
    group_id: int
    date_added: datetime
    user_id: Optional[int]
    team_id: Optional[int]


@dataclass(frozen=True)
class GroupAssigneeRow(CdcMessageRow):
    offset: Optional[int]
    record_deleted: bool
    id: int
    record_content: Union[None, GroupAssigneeRecord]

    @classmethod
    def from_wal(cls,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> GroupAssigneeRow:
        raw_data = dict(zip(columnnames, columnvalues))
        return cls(
            offset=offset,
            id=raw_data['id'],
            record_deleted=False,
            record_content=GroupAssigneeRecord(
                project_id=raw_data['project_id'],
                group_id=raw_data['group_id'],
                date_added=parse_postgres_datetime(raw_data['date_added']),
                user_id=int(raw_data["user_id"]) if raw_data["user_id"] is not None else None,
                team_id=int(raw_data["team_id"]) if raw_data["team_id"] is not None else None,
            )
        )

    @classmethod
    def from_bulk(cls,
        row: Mapping[str, Any],
    ) -> GroupAssigneeRow:
        return cls(
            offset=0,
            id=row['id'],
            record_deleted=False,
            record_content=GroupAssigneeRecord(
                project_id=row['project_id'],
                group_id=row['group_id'],
                date_added=postgres_date_to_clickhouse(row['date_added']),
                user_id=int(row["user_id"]) if row["user_id"] != "" else None,
                team_id=int(row["team_id"]) if row["team_id"] != "" else None,
            )
        )

    def to_clickhouse(self) -> WriterTableRow:
        record = self.record_content
        return {
            'offset': self.offset if self.offset is not None else 0,
            # TODO: project_id and group_id will become part of the key,
            # then we will never have to set them to 0.
            'project_id': 0 if not record else record.project_id,
            'group_id': 0 if not record else record.group_id,
            'id': self.id,
            'record_deleted': 1 if self.record_deleted else 0,
            'date_added': None if not record else record.date_added,
            'user_id': None if not record else record.user_id,
            'team_id': None if not record else record.team_id,
        }


class GroupAssigneeProcessor(CdcProcessor):

    def __init__(self, postgres_table) -> None:
        super().__init__(
            pg_table=postgres_table,
            message_row_class=GroupAssigneeRow,
        )

    def _process_delete(self,
        offset: int,
        key: Mapping[str, Any],
    ) -> Optional[WriterTableRow]:
        key_names = key['keynames']
        key_values = key['keyvalues']
        id = key_values[key_names.index('id')]
        return GroupAssigneeRow(
            offset=offset,
            id=id,
            record_deleted=True,
            record_content=None
        ).to_clickhouse()
