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
class GroupMessageRecord:
    status: int
    last_seen: datetime
    first_seen: datetime
    active_at: Optional[datetime] = None
    first_release_id: Optional[int] = None


@dataclass(frozen=True)
class RawGroupMessageRecord:
    """
    This is a faster verison of GroupMessageRecord that does
    not rely on datetime objects. This is useful for bulk load
    of massive tables to avoid creating and serializing datetime
    objects at every record.
    """

    status: int
    last_seen: str
    first_seen: str
    active_at: Optional[str] = None
    first_release_id: Optional[int] = None


@dataclass(frozen=True)
class GroupedMessageRow(CdcMessageRow):
    offset: Optional[int]
    project_id: int
    id: int
    record_deleted: bool
    record_content: Union[None, GroupMessageRecord, RawGroupMessageRecord]

    @classmethod
    def from_wal(
        cls,
        offset: int,
        columnnames: Sequence[str],
        columnvalues: Sequence[Any],
    ) -> GroupedMessageRow:
        raw_data = dict(zip(columnnames, columnvalues))
        return cls(
            offset=offset,
            project_id=raw_data["project_id"],
            id=raw_data["id"],
            record_deleted=False,
            record_content=GroupMessageRecord(
                status=raw_data["status"],
                last_seen=parse_postgres_datetime(raw_data["last_seen"]),
                first_seen=parse_postgres_datetime(raw_data["first_seen"]),
                active_at=(
                    parse_postgres_datetime(raw_data["active_at"])
                    if raw_data["active_at"]
                    else None
                ),
                first_release_id=raw_data["first_release_id"],
            ),
        )

    @classmethod
    def from_bulk(
        cls,
        row: Mapping[str, Any],
    ) -> GroupedMessageRow:
        return cls(
            offset=None,
            project_id=int(row["project_id"]),
            id=int(row["id"]),
            record_deleted=False,
            record_content=RawGroupMessageRecord(
                status=int(row["status"]),
                last_seen=postgres_date_to_clickhouse(row["last_seen"]),
                first_seen=postgres_date_to_clickhouse(row["first_seen"]),
                active_at=(
                    postgres_date_to_clickhouse(row["active_at"])
                    if row["active_at"]
                    else None
                ),
                first_release_id=int(row["first_release_id"])
                if row["first_release_id"]
                else None,
            ),
        )

    def to_clickhouse(self) -> WriterTableRow:
        record = self.record_content
        return {
            "offset": self.offset if self.offset is not None else 0,
            "project_id": self.project_id,
            "id": self.id,
            "record_deleted": 1 if self.record_deleted else 0,
            "status": None if not record else record.status,
            "last_seen": None if not record else record.last_seen,
            "first_seen": None if not record else record.first_seen,
            "active_at": None if not record else record.active_at,
            "first_release_id": None if not record else record.first_release_id,
        }


class GroupedMessageProcessor(CdcProcessor):
    def __init__(self) -> None:
        postgres_table = "sentry_groupedmessage"
        super(GroupedMessageProcessor, self).__init__(
            pg_table=postgres_table,
            message_row_class=GroupedMessageRow,
        )

    def _process_delete(
        self,
        offset: int,
        key: Mapping[str, Any],
    ) -> Sequence[WriterTableRow]:
        key_names = key["keynames"]
        key_values = key["keyvalues"]
        project_id = key_values[key_names.index("project_id")]
        id = key_values[key_names.index("id")]
        return [
            GroupedMessageRow(
                offset=offset,
                project_id=project_id,
                id=id,
                record_deleted=True,
                record_content=None,
            ).to_clickhouse()
        ]
