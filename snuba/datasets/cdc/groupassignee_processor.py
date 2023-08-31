from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Union

from snuba.datasets.cdc.cdcprocessors import (
    CdcMessageRow,
    CdcProcessor,
    parse_postgres_datetime,
    postgres_date_to_clickhouse,
)
from snuba.writer import WriterTableRow


@dataclass(frozen=True)
class GroupAssigneeRecord:
    date_added: Union[datetime, str]
    user_id: Optional[int]
    team_id: Optional[int]


@dataclass(frozen=True)
class GroupAssigneeRow(CdcMessageRow):
    offset: Optional[int]
    record_deleted: bool
    project_id: int
    group_id: int
    record_content: Union[None, GroupAssigneeRecord]

    @classmethod
    def from_wal(
        cls,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> GroupAssigneeRow:
        raw_data = dict(zip(columnnames, columnvalues))
        return cls(
            offset=offset,
            record_deleted=False,
            project_id=raw_data["project_id"],
            group_id=raw_data["group_id"],
            record_content=GroupAssigneeRecord(
                date_added=parse_postgres_datetime(raw_data["date_added"]),
                user_id=int(raw_data["user_id"])
                if raw_data["user_id"] is not None
                else None,
                team_id=int(raw_data["team_id"])
                if raw_data["team_id"] is not None
                else None,
            ),
        )

    @classmethod
    def from_bulk(
        cls,
        row: Mapping[str, Any],
    ) -> GroupAssigneeRow:
        return cls(
            offset=0,
            record_deleted=False,
            project_id=row["project_id"],
            group_id=row["group_id"],
            record_content=GroupAssigneeRecord(
                date_added=postgres_date_to_clickhouse(row["date_added"]),
                user_id=int(row["user_id"]) if row["user_id"] != "" else None,
                team_id=int(row["team_id"]) if row["team_id"] != "" else None,
            ),
        )

    def to_clickhouse(self) -> WriterTableRow:
        record = self.record_content
        return {
            "offset": self.offset if self.offset is not None else 0,
            "project_id": self.project_id,
            "group_id": self.group_id,
            "record_deleted": 1 if self.record_deleted else 0,
            "date_added": None if not record else record.date_added,
            "user_id": None if not record else record.user_id,
            "team_id": None if not record else record.team_id,
        }


class GroupAssigneeProcessor(CdcProcessor):
    def __init__(self) -> None:
        postgres_table = "sentry_groupasignee"
        super().__init__(
            pg_table=postgres_table,
            message_row_class=GroupAssigneeRow,
        )

    def _process_delete(
        self,
        offset: int,
        key: Mapping[str, Any],
    ) -> Sequence[WriterTableRow]:
        key_names = key["keynames"]
        key_values = key["keyvalues"]
        project_id = key_values[key_names.index("project_id")]
        group_id = key_values[key_names.index("group_id")]
        return [
            GroupAssigneeRow(
                offset=offset,
                record_deleted=True,
                project_id=project_id,
                group_id=group_id,
                record_content=None,
            ).to_clickhouse()
        ]
