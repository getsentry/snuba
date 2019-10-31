from abc import ABC
from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    FixedString,
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
    transaction_column_set = get_dataset('discover') \
        .get_dataset_schemas() \
        .get_read_schema() \
        .get_transactions_only_columns()

    if [col for col in query.get_columns_referenced_in_conditions() if transaction_column_set.get(col)]:
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
        common = ColumnSet([
            ('event_id', FixedString(32)),
            ('project_id', UInt(64)),
            ('timestamp', DateTime()),
            ('deleted', UInt(8)),
            ('retention_days', UInt(16)),
            ('platform', Nullable(String())),
            ('sentry:release', Nullable(String())),
            ('sentry:dist', Nullable(String())),
            ('sentry:user', Nullable(String())),
            # User
            ('user_id', Nullable(String())),
            ('username', Nullable(String())),
            ('email', Nullable(String())),
            ('ip_address', Nullable(String())),
        ])

        return common + self.get_events_only_columns() + self.get_transactions_only_columns()

    def get_events_only_columns(self) -> ColumnSet:
        return ColumnSet([
            ('group_id', Nullable(UInt(64))),
            ('primary_hash', Nullable(FixedString(32))),
            ('type', String()),
            # Promoted tags
            ('level', Nullable(String())),
            ('logger', Nullable(String())),
            ('server_name', Nullable(String())),
            ('transaction', Nullable(String())),
            ('site', Nullable(String())),
            ('url', Nullable(String())),

            ('message', Nullable(String())),
            ('search_message', Nullable(String())),
            ('title', Nullable(String())),
            ('location', Nullable(String())),
            ('culprit', Nullable(String())),
            ('received', Nullable(DateTime())),
            ('geo_country_code', Nullable(String())),
            ('geo_region', Nullable(String())),
            ('geo_city', Nullable(String())),
            ('sdk_name', Nullable(String())),
            ('sdk_integrations', Nullable(Array(String()))),
            ('sdk_version', Nullable(String())),
            ('version', Nullable(String())),
            ('http_method', Nullable(String())),
            ('http_referer', Nullable(String())),
            ('exception_stacks.type', Nullable(String())),
            ('exception_stacks.value', Nullable(String())),
            ('exception_stacks.mechanism_type', Nullable(String())),
            ('exception_stacks.mechanism_handled', Nullable(UInt(8))),
            ('exception_frames.abs_path', Nullable(String())),
            ('exception_frames.filename', Nullable(String())),
            ('exception_frames.package', Nullable(String())),
            ('exception_frames.module', Nullable(String())),
            ('exception_frames.function', Nullable(String())),
            ('exception_frames.in_app', Nullable(UInt(8))),
            ('exception_frames.colno', Nullable(UInt(32))),
            ('exception_frames.lineno', Nullable(UInt(32))),
            ('exception_frames.stack_level', Nullable(UInt(16))),
            ('modules.name', String()),
            ('modules.version', String()),
        ])

    def get_transactions_only_columns(self) -> ColumnSet:
        return ColumnSet([
            ('trace_id', Nullable(UUID())),
            ('span_id', Nullable(UInt(64))),
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
            time_group_columns={},
            time_parse_columns=['timestamp', 'start_ts', 'finish_ts'],
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
            if column_name == 'username':
                return 'user_name'
            if column_name == 'email':
                return 'user_email'
            if column_name == 'transaction':
                return 'transaction_name'
            if self.get_dataset_schemas() \
                    .get_read_schema() \
                    .get_events_only_columns() \
                    .get(column_name):
                return 'NULL'
        else:
            if self.get_dataset_schemas() \
                    .get_read_schema() \
                    .get_transactions_only_columns() \
                    .get(column_name):
                return 'NULL'

        return get_dataset(detected_dataset) \
            .column_expr(column_name, query, parsing_context)
