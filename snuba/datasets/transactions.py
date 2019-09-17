from datetime import timedelta
from typing import Sequence

from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
    WithDefault,
)
from snuba.datasets import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schema import ReplacingMergeTreeSchema
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.query.extensions import PERFORMANCE_EXTENSION_SCHEMA, PROJECT_EXTENSION_SCHEMA
from snuba.query.schema import GENERIC_QUERY_SCHEMA
from snuba.request import RequestSchema
from snuba.schemas import get_time_series_extension_properties


class TransactionsDataset(TimeSeriesDataset):
    def __init__(self):
        columns = ColumnSet([
            ('project_id', UInt(64)),
            ('event_id', UUID()),
            ('trace_id', UUID()),
            ('span_id', UInt(64)),
            ('transaction_name', String()),
            ('transaction_hash', Materialized(
                UInt(64),
                'cityHash64(transaction_name)',
            )),
            ('transaction_op', LowCardinality(String())),
            ('start_ts', DateTime()),
            ('start_ms', UInt(16)),
            ('finish_ts', DateTime()),
            ('finish_ms', UInt(16)),
            ('duration', Materialized(
                UInt(32),
                '((finish_ts - start_ts) * 1000) + (finish_ms - start_ms)',
            )),
            ('platform', LowCardinality(String())),
            ('environment', Nullable(String())),
            ('release', Nullable(String())),
            ('dist', Nullable(String())),
            ('ip_address_v4', Nullable(IPv4())),
            ('ip_address_v6', Nullable(IPv6())),
            ('user', WithDefault(
                String(),
                "''",
            )),
            ('user_id', Nullable(String())),
            ('user_name', Nullable(String())),
            ('user_email', Nullable(String())),
            ('tags', Nested([
                ('key', String()),
                ('value', String()),
            ])),
            ('contexts', Nested([
                ('key', String()),
                ('value', String()),
            ])),
            ('partition', UInt(16)),
            ('offset', UInt(64)),
            ('retention_days', UInt(16)),
            ('deleted', UInt(8)),
        ])

        schema = ReplacingMergeTreeSchema(
            columns=columns,
            local_table_name='transactions_local',
            dist_table_name='transactions_dist',
            order_by='(project_id, toStartOfDay(start_ts), transaction_hash, start_ts, start_ms, trace_id, span_id)',
            partition_by='(retention_days, toMonday(start_ts))',
            version_column='deleted',
            sample_expr=None,
        )

        dataset_schemas = DatasetSchemas(
            read_schema=schema,
            write_schema=schema,
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            processor=TransactionsMessageProcessor(),
            default_topic="events",
            time_group_columns={
                'bucketed_start': 'start_ts',
                'bucketed_end': 'end_ts',
            },
            timestamp_column='start_ts',
        )

    def get_query_schema(self):
        return RequestSchema(GENERIC_QUERY_SCHEMA, {
            'performance': PERFORMANCE_EXTENSION_SCHEMA,
            'project': PROJECT_EXTENSION_SCHEMA,
            'timeseries': get_time_series_extension_properties(
                default_granularity=3600,
                default_window=timedelta(days=5),
            ),
        })

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['event_id', 'project_id']
