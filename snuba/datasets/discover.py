from datetime import timedelta
from typing import Mapping, Sequence

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
from snuba.datasets.events import EventsDataset
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_table import SelectedTableQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, ReadableStorage
from snuba.datasets.transactions import TransactionsDataset
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.processors.apdex_processor import ApdexProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.impact_processor import ImpactProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition

EVENTS = "events"
TRANSACTIONS = "transactions"


def detect_table(
    query: Query, events_only_columns: ColumnSet, transactions_only_columns: ColumnSet
) -> str:
    """
    Given a query, we attempt to guess whether it is better to fetch data from the
    "events" or "transactions" storage. This is going to be wrong in some cases.
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

    # Check for any conditions that reference a table specific field
    condition_columns = query.get_columns_referenced_in_conditions()
    if any(events_only_columns.get(col) for col in condition_columns):
        return EVENTS
    if any(transactions_only_columns.get(col) for col in condition_columns):
        return TRANSACTIONS

    # Check for any other references to a table specific field
    all_referenced_columns = query.get_all_referenced_columns()
    if any(events_only_columns.get(col) for col in all_referenced_columns):
        return EVENTS
    if any(transactions_only_columns.get(col) for col in all_referenced_columns):
        return TRANSACTIONS

    # Use events by default
    return EVENTS


class DiscoverQueryStorageSelector(QueryStorageSelector):
    def __init__(
        self,
        events_table: ReadableStorage,
        abstract_events_columns: ColumnSet,
        transactions_table: ReadableStorage,
        abstract_transactions_columns: ColumnSet,
    ) -> None:
        self.__events_table = events_table
        # Columns from the abstract model that map to storage columns present only
        # in the Events table
        self.__abstract_events_columns = abstract_events_columns
        self.__transactions_table = transactions_table
        # Columns from the abstract model that map to storage columns present only
        # in the Transactions table
        self.__abstract_transactions_columns = abstract_transactions_columns

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> ReadableStorage:
        table = detect_table(
            query, self.__abstract_events_columns, self.__abstract_transactions_columns,
        )
        return self.__events_table if table == EVENTS else self.__transactions_table


class DiscoverDataset(TimeSeriesDataset):
    """
    Dataset for the Discover product that maps the columns of Events and
    Transactions into a standard format and sends a query to one of the 2 tables
    depending on the conditions detected.

    It is based on two storages. One for events and one for transactions.
    """

    def __init__(self) -> None:
        self.__common_columns = ColumnSet(
            [
                ("event_id", FixedString(32)),
                ("project_id", UInt(64)),
                ("type", Nullable(String())),
                ("timestamp", DateTime()),
                ("platform", Nullable(String())),
                ("environment", Nullable(String())),
                ("release", Nullable(String())),
                ("dist", Nullable(String())),
                ("user", Nullable(String())),
                ("transaction", Nullable(String())),
                ("message", Nullable(String())),
                ("title", Nullable(String())),
                # User
                ("user_id", Nullable(String())),
                ("username", Nullable(String())),
                ("email", Nullable(String())),
                ("ip_address", Nullable(String())),
                # SDK
                ("sdk_name", Nullable(String())),
                ("sdk_version", Nullable(String())),
                # geo location context
                ("geo_country_code", Nullable(String())),
                ("geo_region", Nullable(String())),
                ("geo_city", Nullable(String())),
                # Other tags and context
                ("tags", Nested([("key", String()), ("value", String())])),
                ("contexts", Nested([("key", String()), ("value", String())])),
            ]
        )

        self.__events_columns = ColumnSet(
            [
                ("group_id", Nullable(UInt(64))),
                ("primary_hash", Nullable(FixedString(32))),
                # Promoted tags
                ("level", Nullable(String())),
                ("logger", Nullable(String())),
                ("server_name", Nullable(String())),
                ("site", Nullable(String())),
                ("url", Nullable(String())),
                ("search_message", Nullable(String())),
                ("location", Nullable(String())),
                ("culprit", Nullable(String())),
                ("received", Nullable(DateTime())),
                ("sdk_integrations", Nullable(Array(String()))),
                ("version", Nullable(String())),
                ("http_method", Nullable(String())),
                ("http_referer", Nullable(String())),
                # exception interface
                (
                    "exception_stacks",
                    Nested(
                        [
                            ("type", Nullable(String())),
                            ("value", Nullable(String())),
                            ("mechanism_type", Nullable(String())),
                            ("mechanism_handled", Nullable(UInt(8))),
                        ]
                    ),
                ),
                (
                    "exception_frames",
                    Nested(
                        [
                            ("abs_path", Nullable(String())),
                            ("filename", Nullable(String())),
                            ("package", Nullable(String())),
                            ("module", Nullable(String())),
                            ("function", Nullable(String())),
                            ("in_app", Nullable(UInt(8))),
                            ("colno", Nullable(UInt(32))),
                            ("lineno", Nullable(UInt(32))),
                            ("stack_level", UInt(16)),
                        ]
                    ),
                ),
                ("modules", Nested([("name", String()), ("version", String())])),
            ]
        )

        self.__transactions_columns = ColumnSet(
            [
                ("trace_id", Nullable(UUID())),
                ("span_id", Nullable(UInt(64))),
                ("transaction_hash", Nullable(UInt(64))),
                ("transaction_op", Nullable(String())),
                ("transaction_status", Nullable(UInt(8))),
                ("duration", Nullable(UInt(32))),
            ]
        )

        # TODO: Move the storage definition out of events and transactions dataset so
        # we can reuse the class itself and avoid the isinstance call.
        events_dataset = get_dataset(EVENTS)
        assert isinstance(events_dataset, EventsDataset)
        transactions_dataset = get_dataset(TRANSACTIONS)
        assert isinstance(transactions_dataset, TransactionsDataset)
        storage_selector = DiscoverQueryStorageSelector(
            events_table=events_dataset.get_storage(),
            abstract_events_columns=self.__events_columns,
            transactions_table=transactions_dataset.get_storage(),
            abstract_transactions_columns=self.__transactions_columns,
        )

        super().__init__(
            storages=[
                events_dataset.get_storage(),
                transactions_dataset.get_storage(),
            ],
            query_plan_builder=SelectedTableQueryPlanBuilder(
                selector=storage_selector, post_processors=[PrewhereProcessor()],
            ),
            abstract_column_set=(
                self.__common_columns
                + self.__events_columns
                + self.__transactions_columns
            ),
            writable_storage=None,
            time_group_columns={},
            time_parse_columns=["timestamp"],
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            # Apdex and Impact seem very good candidates for
            # being defined by the Transaction entity when it will
            # exist, so it would run before Storage selection.
            ApdexProcessor(),
            ImpactProcessor(),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(
                    project_column="project_id", replacer_state_name=None,
                )
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
        detected_dataset = detect_table(
            query, self.__events_columns, self.__transactions_columns
        )

        if detected_dataset == TRANSACTIONS:
            if column_name == "time":
                return self.time_expr("finish_ts", query.get_granularity(), table_alias)
            if column_name == "type":
                return "'transaction'"
            if column_name == "timestamp":
                return "finish_ts"
            if column_name == "username":
                return "user_name"
            if column_name == "email":
                return "user_email"
            if column_name == "transaction":
                return "transaction_name"
            if column_name == "message":
                return "transaction_name"
            if column_name == "title":
                return "transaction_name"
            if column_name == "group_id":
                # TODO: We return 0 here instead of NULL so conditions like group_id
                # in (1, 2, 3) will work, since Clickhouse won't run a query like:
                # SELECT (NULL AS group_id) FROM transactions WHERE group_id IN (1, 2, 3)
                # When we have the query AST, we should solve this by transforming the
                # nonsensical conditions instead.
                return "0"
            if column_name == "geo_country_code":
                column_name = "contexts[geo.country_code]"
            if column_name == "geo_region":
                column_name = "contexts[geo.region]"
            if column_name == "geo_city":
                column_name = "contexts[geo.city]"
            if self.__events_columns.get(column_name):
                return "NULL"
        else:
            if column_name == "time":
                return self.time_expr("timestamp", query.get_granularity(), table_alias)
            if column_name == "release":
                column_name = "tags[sentry:release]"
            if column_name == "dist":
                column_name = "tags[sentry:dist]"
            if column_name == "user":
                column_name = "tags[sentry:user]"
            if self.__transactions_columns.get(column_name):
                return "NULL"

        return get_dataset(detected_dataset).column_expr(
            column_name, query, parsing_context
        )


class InvalidDataset(Exception):
    pass
