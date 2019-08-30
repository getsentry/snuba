from datetime import datetime
import uuid

from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets import Dataset
from snuba.processor import _ensure_valid_date, MessageProcessor, _unicodify
from snuba.datasets.schema import MergeTreeSchema
from snuba import settings


class OutcomesProcessor(MessageProcessor):
    def process_message(self, value, metadata):
        assert isinstance(value, dict)
        v_uuid = value.get('event_id')
        message = {
            'org_id': value.get('org_id', 0),
            'project_id': value.get('project_id', 0),
            'key_id': value.get('key_id'),
            'timestamp': _ensure_valid_date(
                datetime.strptime(value['timestamp'], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            'outcome': value['outcome'],
            'reason': _unicodify(value.get('reason')),
            'event_id': str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return (self.INSERT, message)


class OutcomesDataset(Dataset):
    """
    Tracks event ingesiton outcomes in Sentry.
    """

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
            # TODO: change to outcomes.raw_local when we add multi DB support
            local_table_name='outcomes_raw_local',
            dist_table_name='outcomes_raw_dist',
            order_by='(org_id, project_id, timestamp)',
            partition_by='(toMonday(timestamp))',
            settings={
                'index_granularity': 16384
            })

        super(OutcomesDataset, self).__init__(
            schema=schema,
            processor=OutcomesProcessor(),
            default_topic="outcomes",
        )
