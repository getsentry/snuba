from datetime import datetime
import uuid

from snuba.clickhouse import (
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets import Dataset
from snuba.processor import _ensure_valid_date, MessageProcessor
from snuba.datasets.schema import MergeTreeSchema
from snuba import settings


class OutcomesProcessor(MessageProcessor):
    def process_message(self, value, metadata):
        assert isinstance(value, dict)
        message = {
            'org_id': value['org_id'],
            'project_id': value['project_id'],
            'key_id': value['key_id'],
            'timestamp': _ensure_valid_date(
                datetime.strptime(value['timestamp'], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            'outcome': value['outcome'],
            'reason': value['reason'],
            'event_id': str(uuid.UUID(value['event_id'])),
        }

        return (self.INSERT, message)


class OutcomesDataset(Dataset):
    def __init__(self):
        columns = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', Nullable(UInt(64))),
            ('timestamp', DateTime()),
            ('outcome', UInt(8)),
            ('reason', LowCardinality(Nullable(String()))),
            ('event_id', Nullable(UUID())),
        ])

        schema = MergeTreeSchema(
            columns=columns,
            local_table_name='raw_local',
            dist_table_name='raw_dist',
            order_by='(org_id, project_id, timestamp)',
            partition_by='(toMonday(timestamp))',
            settings={
                'index_granularity': 16384
            })

        super(OutcomesDataset, self).__init__(
            schema=schema,
            processor=OutcomesProcessor(),
            default_topic="outcomes",
            default_replacement_topic=None,
            default_commit_log_topic=None,
        )
