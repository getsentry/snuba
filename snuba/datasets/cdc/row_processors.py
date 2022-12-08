from snuba.datasets.cdc.groupassignee_processor import GroupAssigneeRow
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageRow
from snuba.datasets.storages.storage_key import StorageKey
from snuba.snapshots import SnapshotTableRow
from snuba.writer import WriterTableRow


def __group_assignee_row_processor(row: SnapshotTableRow) -> WriterTableRow:
    return GroupAssigneeRow.from_bulk(row).to_clickhouse()


def __grouped_message_row_processor(row: SnapshotTableRow) -> WriterTableRow:
    return GroupedMessageRow.from_bulk(row).to_clickhouse()


CDC_ROW_PROCESSORS = {
    StorageKey.GROUPASSIGNEES: __group_assignee_row_processor,
    StorageKey.GROUPEDMESSAGES: __grouped_message_row_processor,
}
