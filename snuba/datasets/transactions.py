from datetime import timedelta
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Tuple, Union

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
from snuba.datasets.dataset import ColumnSplitSpec, TimeSeriesDataset
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schemas.tables import MigrationSchemaColumn, ReplacingMergeTreeSchema
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor


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


def transactions_migrations(clickhouse_table: str, current_schema: Mapping[str, MigrationSchemaColumn]) -> Sequence[str]:
    ret = []
    duration_col = current_schema.get("duration")
    if duration_col and duration_col.default_type == "MATERIALIZED":
        ret.append("ALTER TABLE %s MODIFY COLUMN duration UInt32" % clickhouse_table)
    return ret


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
            ('duration', UInt(32)),
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
            mandatory_conditions=[],
            order_by='(project_id, toStartOfDay(start_ts), transaction_hash, start_ts, start_ms, trace_id, span_id)',
            partition_by='(retention_days, toMonday(start_ts))',
            version_column='deleted',
            sample_expr=None,
            migration_function=transactions_migrations,
        )

        dataset_schemas = DatasetSchemas(
            read_schema=schema,
            write_schema=schema,
        )

        self.__tags_processor = TagColumnProcessor(
            columns=columns,
            promoted_columns=self._get_promoted_columns(),
            column_tag_map=self._get_column_tag_map(),
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
            time_parse_columns=('start_ts', 'finish_ts')
        )

    def _get_promoted_columns(self):
        # TODO: Support promoted tags
        return {
            'tags': frozenset(),
            'contexts': frozenset(),
        }

    def _get_column_tag_map(self):
        # TODO: Support promoted tags
        return {
            'tags': {},
            'contexts': {},
        }

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            'project': ProjectExtension(
                processor=ProjectExtensionProcessor(project_column="project_id")
            ),
            'timeseries': TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column='start_ts',
            ),
        }

    def column_expr(self, column_name, query: Query, parsing_context: ParsingContext, table_alias: str=""):
        # TODO remove these casts when clickhouse-driver is >= 0.0.19
        if column_name == 'ip_address_v4':
            return 'IPv4NumToString(ip_address_v4)'
        if column_name == 'ip_address_v6':
            return 'IPv6NumToString(ip_address_v6)'
        if column_name == 'ip_address':
            return f"coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))"
        if column_name == 'event_id':
            return 'replaceAll(toString(event_id), \'-\', \'\')'
        processed_column = self.__tags_processor.process_column_expression(column_name, query, parsing_context, table_alias)
        if processed_column:
            # If processed_column is None, this was not a tag/context expression
            return processed_column
        return super().column_expr(column_name, query, parsing_context)

    def get_split_query_spec(self) -> Union[None, ColumnSplitSpec]:
        return ColumnSplitSpec(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="start_ts",
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['event_id', 'project_id']
