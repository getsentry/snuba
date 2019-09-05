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
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.processor import _ensure_valid_date, MessageProcessor, _unicodify
from snuba.datasets.schema import MergeTreeSchema, SummingMergeTreeSchema, MaterializedViewSchema
from snuba import settings


WRITE_SCHEMA_LOCAL_TABLE_NAME = 'outcomes_raw_local'
WRITE_SCHEMA_DIST_TABLE_NAME = 'outcomes_raw_dist'
READ_SCHEMA_LOCAL_TABLE_NAME = 'outcomes_hourly_local'
READ_SCHEMA_DIST_TABLE_NAME = 'outcomes_hourly_dist'


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
        write_columns = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', Nullable(UInt(64))),
            ('timestamp', DateTime()),
            ('outcome', UInt(8)),
            ('reason', LowCardinality(Nullable(String()))),
            ('event_id', Nullable(UUID())),
        ])

        write_schema = MergeTreeSchema(
            columns=write_columns,
            # TODO: change to outcomes.raw_local when we add multi DB support
            local_table_name=WRITE_SCHEMA_LOCAL_TABLE_NAME,
            dist_table_name=WRITE_SCHEMA_DIST_TABLE_NAME,
            order_by='(org_id, project_id, timestamp)',
            partition_by='(toMonday(timestamp))',
            settings={
                'index_granularity': 16384
            })

        read_columns = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', UInt(64)),
            ('timestamp', DateTime()),
            ('outcome', UInt(8)),
            ('reason', LowCardinality(String())),
            ('times_seen', UInt(64)),
        ])

        read_schema = SummingMergeTreeSchema(
            columns=read_columns,
            local_table_name=READ_SCHEMA_LOCAL_TABLE_NAME,
            dist_table_name=READ_SCHEMA_DIST_TABLE_NAME,
            order_by='(org_id, project_id, key_id, outcome, reason, timestamp)',
            partition_by='(toMonday(timestamp))',
            settings={
                'index_granularity': 256
            }
        )

        materialized_view_columns = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', UInt(64)),
            ('timestamp', DateTime()),
            ('outcome', UInt(8)),
            ('reason', String()),
            ('times_seen', UInt(64)),
        ])

        query = """
               SELECT
                   org_id,
                   project_id,
                   ifNull(key_id, 0) AS key_id,
                   toStartOfHour(timestamp) AS timestamp,
                   outcome,
                   ifNull(reason, 'none') AS reason,
                   count() AS times_seen
               FROM %(source_table_name)s
               GROUP BY org_id, project_id, key_id, timestamp, outcome, reason
               """

        materialized_view = MaterializedViewSchema(
            local_materialized_view_name='outcomes_mv_hourly_local',
            dist_materialized_view_name='outcomes_mv_hourly_dist',
            columns=materialized_view_columns,
            query=query,
            local_source_table_name=WRITE_SCHEMA_LOCAL_TABLE_NAME,
            local_destination_table_name=READ_SCHEMA_LOCAL_TABLE_NAME,
            dist_source_table_name=WRITE_SCHEMA_DIST_TABLE_NAME,
            dist_destination_table_name=READ_SCHEMA_DIST_TABLE_NAME
        )

        dataset_schemas = DatasetSchemas(
            read_schema=read_schema,
            write_schema=write_schema,
            intermediary_schemas=[materialized_view]
        )

        super(OutcomesDataset, self).__init__(
            dataset_schemas=dataset_schemas,
            processor=OutcomesProcessor(),
            default_topic="outcomes",
        )
