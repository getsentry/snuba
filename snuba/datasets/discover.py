from abc import ABC
from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    FixedString,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas import Schema
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition

EVENTS = 'events'
TRANSACTIONS = 'transactions'
# TODO: replace with get_transaction_only_columns()
TRANSACTIONS_ONLY_COLUMNS = [
    'trace_id',
    'span_id',
    'transaction_name',
    'transaction_hash',
    'transaction_op',
    'start_ts',
    'start_ms',
    'finish_ts',
    'finish_ms',
    'duration',
]

EVENTS_ONLY_COLUMNS = [
    'group_id',
    'primary_hash',
    'message',
    'search_message',
    'title',
    'location',
    'transaction',
    'culprit',
    'exception_stacks.type',
    'exception_stacks.value',
    'exception_stacks.mechanism_type',
    'exception_stacks.mechanism_handled',
    'exception_frames.abs_path',
    'exception_frames.filename',
    'exception_frames.package',
    'exception_frames.module',
    'exception_frames.function',
    'exception_frames.in_app',
    'exception_frames.colno',
    'exception_frames.lineno',
    'exception_frames.stack_level',
]


def detect_dataset(query: Query) -> str:
    """
    Given a query, we attempt to guess whether it is better to fetch data from the
    "events" or "transactions" dataset. This is going to be wrong in some cases.
    """
    # First check for a top level condition that matches either type = transaction
    # type != transaction.
    conditions = query.get_conditions()
    if conditions:
        for idx, condition in enumerate(conditions):
            if is_condition(condition):
                if tuple(condition) == ('type', '!=', 'transaction'):
                    return EVENTS
                elif tuple(condition) == ('type', '=', 'transaction'):
                    return TRANSACTIONS

    # If there is a condition that references a transactions only field, just switch
    # to the transactions dataset
    if [col for col in TRANSACTIONS_ONLY_COLUMNS if col in query.get_columns_referenced_in_conditions()]:
        return TRANSACTIONS

    # Use events by default
    return EVENTS


class DiscoverProcessor(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        """
        Switches the data source from the default (Events) to the transactions
        table if a transaction specific column is detected.
        Sets all the other columns to none

        """
        detected_dataset = detect_dataset(query)

        source = get_dataset(detected_dataset) \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()
        query.set_data_source(source)


class DiscoverSchema(Schema, ABC):
    def get_data_source(self):
        """
        This is a placeholder, we switch out the data source in the processor
        depending on the detected dataset
        """
        return None

    def get_columns(self) -> ColumnSet:
        common = ColumnSet([])  # TODO: Fill out

        return common + self.get_events_only_columns() + self.get_transactions_only_columns()

    def get_events_only_columns(self) -> ColumnSet:
        return ColumnSet([
            ('group_id', Nullable(UInt(64))),
            ('primary_hash', Nullable(FixedString(32))),
            ('message', Nullable(String())),
            ('search_message', Nullable(String())),
            ('title', Nullable(String())),
            ('location', Nullable(String())),
            ('transaction', Nullable(String())),
            ('culprit', Nullable(String())),
            ('site', Nullable(String())),
            ('url', Nullable(String())),
            ('exception_stacks', Nested([
                ('type', Nullable(String())),
                ('value', Nullable(String())),
                ('mechanism_type', Nullable(String())),
                ('mechanism_handled', Nullable(UInt(8))),
            ])),
            ('exception_frames', Nested([
                ('abs_path', Nullable(String())),
                ('filename', Nullable(String())),
                ('package', Nullable(String())),
                ('module', Nullable(String())),
                ('function', Nullable(String())),
                ('in_app', Nullable(UInt(8))),
                ('colno', Nullable(UInt(32))),
                ('lineno', Nullable(UInt(32))),
                ('stack_level', UInt(16)),
            ])),
        ])

    def get_transactions_only_columns(self) -> ColumnSet:
        return ColumnSet([
            ('trace_id', Nullable(UUID())),
            ('span_id', Nullable(UInt(64))),
            ('transaction_name', String()),
            ('transaction_hash', Nullable(UInt(64))),
            ('transaction_op', Nullable(String())),
            ('start_ts', Nullable(DateTime())),
            ('start_ms', Nullable(UInt(16))),
            ('finish_ts', Nullable(DateTime())),
            ('finish_ms', Nullable(UInt(16))),
            ('duration', Nullable(UInt(32))),
        ])


class DiscoverDataset(TimeSeriesDataset):
    """
    Experimental dataset for Discover
    that coerces the columns of Events and Transactions into the same format
    and sends a query to either one.

    Currently does this by switching between events and transactions tables
    depending on the conditions in the provided query.
    """

    def __init__(self) -> None:
        super().__init__(
            dataset_schemas=DatasetSchemas(
                read_schema=DiscoverSchema(),
                write_schema=None,
            ),
            time_group_columns={
                'time': 'timestamp',
            },
            time_parse_columns=['timestamp'],
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            DiscoverProcessor(),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            'project': ProjectExtension(
                processor=ProjectWithGroupsProcessor(project_column='project_id')
            ),
            'timeseries': TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column='timestamp',
            ),
        }

    def column_expr(self, column_name, query: Query, parsing_context: ParsingContext):
        detected_dataset = detect_dataset(query)

        if detected_dataset == TRANSACTIONS:
            if column_name == 'type':
                return "'transaction'"
            if column_name == 'timestamp':
                return 'finish_ts'
            if column_name == 'sentry:release':
                return 'release'
            if column_name == 'sentry:dist':
                return 'dist'
            if column_name == 'sentry:user':
                return 'user'
            if column_name in EVENTS_ONLY_COLUMNS:
                return 'NULL'
        else:
            if column_name in TRANSACTIONS_ONLY_COLUMNS:
                return 'NULL'

        return get_dataset(detected_dataset) \
            .column_expr(column_name, query, parsing_context)
