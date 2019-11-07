from datetime import timedelta
from typing import Mapping, Optional, Sequence

from snuba.clickhouse.columns import (
    Array,
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
from snuba.datasets.schemas import Schema, RelationalSource
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition

EVENTS = "events"
TRANSACTIONS = "transactions"


def detect_dataset(query: Query, transactions_columns: ColumnSet) -> str:
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
                if tuple(condition) == ("type", "!=", "transaction"):
                    return EVENTS
                elif tuple(condition) == ("type", "=", "transaction"):
                    return TRANSACTIONS

    # If there is a condition that references a transactions only field, just switch
    # to the transactions dataset
    if [
        col
        for col in query.get_columns_referenced_in_conditions()
        if transactions_columns.get(col)
    ]:
        return TRANSACTIONS

    # Use events by default
    return EVENTS


class DiscoverSource(RelationalSource):
    def __init__(
        self, columns: ColumnSet, table_source: RelationalSource
    ) -> None:
        self.__columns = columns
        self.__table_source = table_source

    def format_from(self) -> str:
        if self.__table_source:
            return self.__table_source.format_from()
        raise InvalidDataset

    def get_columns(self) -> ColumnSet:
        return self.__columns

    def get_mandatory_conditions(self) -> Sequence[Condition]:
        if self.__table_source:
            return self.__table_source.get_mandatory_conditions()
        return []

    def set_table_source(self, dataset_name: str) -> None:
        self.__table_source = (
            get_dataset(dataset_name)
            .get_dataset_schemas()
            .get_read_schema()
            .get_data_source()
        )


class DatasetSelector(QueryProcessor):
    """
    Switches the table source to either Events or Transactions, depending on
    the detected dataset.
    """

    def __init__(
        self, discover_source: RelationalSource, transactions_columns: ColumnSet
    ) -> None:
        self.__discover_source = discover_source
        self.__transactions_columns = transactions_columns

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        detected_dataset = detect_dataset(query, self.__transactions_columns)
        source = query.get_data_source()
        assert isinstance(source, DiscoverSource)
        source.set_table_source(detected_dataset)


class DiscoverSchema(Schema):
    def __init__(
        self, table_source, common_columns, events_columns, transactions_columns
    ) -> None:
        self.__common_columns = common_columns
        self.__events_columns = events_columns
        self.__transactions_columns = transactions_columns
        self.__data_source = DiscoverSource(self.get_columns(), table_source)

    def get_data_source(self) -> DiscoverSource:
        return self.__data_source

    def get_columns(self) -> ColumnSet:
        return (
            self.__common_columns + self.__events_columns + self.__transactions_columns
        )


class DiscoverDataset(TimeSeriesDataset):
    """
    Experimental dataset for Discover that maps the columns of Events and
    Transactions into a standard format and sends a query to one of the 2 tables
    depending on the conditions detected.
    """

    def __init__(self) -> None:
        self.__common_columns = ColumnSet(
            [
                ("event_id", FixedString(32)),
                ("project_id", UInt(64)),
                ("timestamp", DateTime()),
                ("platform", Nullable(String())),
                ("environment", Nullable(String())),
                ("sentry:release", Nullable(String())),
                ("sentry:dist", Nullable(String())),
                ("sentry:user", Nullable(String())),
                ("transaction", Nullable(String())),
                # User
                ("user_id", Nullable(String())),
                ("username", Nullable(String())),
                ("email", Nullable(String())),
                ("ip_address", Nullable(String())),
                # Other tags and context
                ("tags", Nested([("key", String()), ("value", String()), ])),
                ("contexts", Nested([("key", String()), ("value", String()), ])),
            ]
        )

        self.__events_columns = ColumnSet(
            [
                ("group_id", Nullable(UInt(64))),
                ("primary_hash", Nullable(FixedString(32))),
                ("type", Nullable(String())),
                # Promoted tags
                ("level", Nullable(String())),
                ("logger", Nullable(String())),
                ("server_name", Nullable(String())),
                ("site", Nullable(String())),
                ("url", Nullable(String())),
                ("message", Nullable(String())),
                ("search_message", Nullable(String())),
                ("title", Nullable(String())),
                ("location", Nullable(String())),
                ("culprit", Nullable(String())),
                ("received", Nullable(DateTime())),
                ("geo_country_code", Nullable(String())),
                ("geo_region", Nullable(String())),
                ("geo_city", Nullable(String())),
                ("sdk_name", Nullable(String())),
                ("sdk_integrations", Nullable(Array(String()))),
                ("sdk_version", Nullable(String())),
                ("version", Nullable(String())),
                ("http_method", Nullable(String())),
                ("http_referer", Nullable(String())),
                ("exception_stacks.type", Nullable(String())),
                ("exception_stacks.value", Nullable(String())),
                ("exception_stacks.mechanism_type", Nullable(String())),
                ("exception_stacks.mechanism_handled", Nullable(UInt(8))),
                ("exception_frames.abs_path", Nullable(String())),
                ("exception_frames.filename", Nullable(String())),
                ("exception_frames.package", Nullable(String())),
                ("exception_frames.module", Nullable(String())),
                ("exception_frames.function", Nullable(String())),
                ("exception_frames.in_app", Nullable(UInt(8))),
                ("exception_frames.colno", Nullable(UInt(32))),
                ("exception_frames.lineno", Nullable(UInt(32))),
                ("exception_frames.stack_level", Nullable(UInt(16))),
                ("modules.name", String()),
                ("modules.version", String()),
            ]
        )

        self.__transactions_columns = ColumnSet(
            [
                ("trace_id", Nullable(UUID())),
                ("span_id", Nullable(UInt(64))),
                ("transaction_hash", Nullable(UInt(64))),
                ("transaction_op", Nullable(String())),
                # TODO: Time columns below will need to be aligned more closely to the
                # names in events once we figure out how timeseries queries will work
                ("start_ts", Nullable(DateTime())),
                ("start_ms", Nullable(UInt(16))),
                ("finish_ts", Nullable(DateTime())),
                ("finish_ms", Nullable(UInt(16))),
                ("duration", Nullable(UInt(32))),
            ]
        )

        super().__init__(
            dataset_schemas=DatasetSchemas(
                read_schema=DiscoverSchema(
                    table_source=None,
                    common_columns=self.__common_columns,
                    events_columns=self.__events_columns,
                    transactions_columns=self.__transactions_columns,
                ),
                write_schema=None,
            ),
            time_group_columns={"time": "timestamp", "bucketed_end": "finish_ts"},
            time_parse_columns=["timestamp", "start_ts", "finish_ts"],
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        discover_source = self.get_dataset_schemas().get_read_schema().get_data_source()

        return [
            DatasetSelector(discover_source, self.__transactions_columns),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(project_column="project_id")
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        detected_dataset = detect_dataset(query, self.__transactions_columns)

        if detected_dataset == TRANSACTIONS:
            if column_name == "type":
                return "'transaction'"
            if column_name == "timestamp":
                return "finish_ts"
            if column_name == "sentry:release":
                return "release"
            if column_name == "sentry:dist":
                return "dist"
            if column_name == "sentry:user":
                return "user"
            if column_name == "username":
                return "user_name"
            if column_name == "email":
                return "user_email"
            if column_name == "transaction":
                return "transaction_name"
            if self.__events_columns.get(column_name):
                return "NULL"
        else:
            if self.__transactions_columns.get(column_name):
                return "NULL"

        return get_dataset(detected_dataset).column_expr(
            column_name, query, parsing_context
        )


class InvalidDataset(Exception):
    pass
