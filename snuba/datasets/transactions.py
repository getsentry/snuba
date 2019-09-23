from datetime import timedelta
from typing import Any, Mapping, MutableMapping, Optional, Sequence

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
from snuba.writer import BatchWriter
from snuba.datasets import TimeSeriesDataset
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.query.extensions import (
    PerformanceExtension,
    ProjectExtension,
    QueryExtension,
)
from snuba.query.timeseries import TimeSeriesExtension


class TransactionsTableWriter(TableWriter):
    def __update_options(self,
        options: Optional[MutableMapping[str, Any]]=None,
    ) -> MutableMapping[str, Any]:
        if options is None:
            options = {}
        if "insert_allow_materialized_columns" not in options:
            options["insert_allow_materialized_columns"] = 1
        return options

    def get_writer(self,
        options: Optional[MutableMapping[str, Any]]=None,
        table_name: Optional[str]=None,
    ) -> BatchWriter:
        return super().get_writer(
            self.__update_options(options),
            table_name,
        )

    def get_bulk_writer(self,
        options: Optional[MutableMapping[str, Any]]=None,
        table_name: Optional[str]=None,
    ) -> BatchWriter:
        return super().get_bulk_writer(
            self.__update_options(options),
            table_name,
        )


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
            table_writer=TransactionsTableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=TransactionsMessageProcessor(),
                    default_topic="events",
                ),
            ),
            time_group_columns={
                'bucketed_start': 'start_ts',
                'bucketed_end': 'finish_ts',
            },
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            'performance': PerformanceExtension(),
            'project': ProjectExtension(),
            'timeseries': TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column='start_ts',
            ),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['event_id', 'project_id']
