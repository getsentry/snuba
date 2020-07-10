import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping, Optional, Sequence

from snuba import environment, state
from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Nested,
    Nullable,
    String,
    UInt,
)
from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import ColumnMapper
from snuba.clickhouse.translators.snuba.mappers import ColumnToLiteral, ColumnToMapping
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.events import event_translator
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import (
    QueryStorageSelector,
    ReadableStorage,
    StorageAndMappers,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.transactions import transaction_translator
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.apdex_processor import ApdexProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.failure_rate_processor import FailureRateProcessor
from snuba.query.processors.impact_processor import ImpactProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition, qualified_column
from snuba.utils.metrics.backends.wrapper import MetricsWrapper

EVENTS = "events"
TRANSACTIONS = "transactions"

metrics = MetricsWrapper(environment.metrics, "api.discover")
logger = logging.getLogger(__name__)


def detect_table(
    query: Query,
    events_only_columns: ColumnSet,
    transactions_only_columns: ColumnSet,
    track_bad_queries: bool,
) -> str:
    """
    Given a query, we attempt to guess whether it is better to fetch data from the
    "events" or "transactions" storage. This is going to be wrong in some cases.
    """
    event_columns = set()
    transaction_columns = set()
    for col in query.get_all_referenced_columns():
        if events_only_columns.get(col):
            event_columns.add(col)
        elif transactions_only_columns.get(col):
            transaction_columns.add(col)
    for col in query.get_all_ast_referenced_columns():
        if events_only_columns.get(col.column_name):
            event_columns.add(col.column_name)
        elif transactions_only_columns.get(col.column_name):
            transaction_columns.add(col.column_name)

    selected_table = None

    # First check for a top level condition that matches either type = transaction
    # type != transaction.
    conditions = query.get_conditions()
    if conditions:
        for idx, condition in enumerate(conditions):
            if is_condition(condition):
                if tuple(condition) == ("type", "=", "error"):
                    selected_table = EVENTS
                elif tuple(condition) == ("type", "=", "transaction"):
                    selected_table = TRANSACTIONS

    if not selected_table:
        # Check for any conditions that reference a table specific field
        condition_columns = query.get_columns_referenced_in_conditions()
        if any(events_only_columns.get(col) for col in condition_columns):
            selected_table = EVENTS
        if any(transactions_only_columns.get(col) for col in condition_columns):
            selected_table = TRANSACTIONS

    if not selected_table:
        # Check for any other references to a table specific field
        if len(event_columns) > 0:
            selected_table = EVENTS
        elif len(transaction_columns) > 0:
            selected_table = TRANSACTIONS

    # Use events by default
    if selected_table is None:
        selected_table = EVENTS

    if track_bad_queries:
        event_mismatch = event_columns and selected_table == TRANSACTIONS
        transaction_mismatch = transaction_columns and selected_table == EVENTS
        if event_mismatch or transaction_mismatch:
            missing_columns = ",".join(
                sorted(event_columns if event_mismatch else transaction_columns)
            )
            metrics.increment(
                "query.impossible",
                tags={
                    "selected_table": selected_table,
                    "missing_columns": missing_columns,
                },
            )
            logger.warning("Discover generated impossible query", exc_info=True)
        else:
            metrics.increment("query.success")

    return selected_table


@dataclass(frozen=True)
class DefaultNoneColumnMapper(ColumnMapper):
    """
    This maps a list of column names to None (NULL in SQL) as it is done
    in the discover column_expr method today. It should not be used for
    any other reason or use case, thus it should not be moved out of
    the discover dataset file.
    """

    columns: ColumnSet

    def attempt_map(
        self, expression: Column, children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Literal]:
        if expression.column_name in self.columns:
            return Literal(
                alias=expression.alias
                or qualified_column(
                    expression.column_name, expression.table_name or ""
                ),
                value=None,
            )
        else:
            return None


class DiscoverQueryStorageSelector(QueryStorageSelector):
    def __init__(
        self,
        events_table: ReadableStorage,
        events_ro_table: ReadableStorage,
        abstract_events_columns: ColumnSet,
        transactions_table: ReadableStorage,
        abstract_transactions_columns: ColumnSet,
    ) -> None:
        self.__events_table = events_table
        self.__events_ro_table = events_ro_table
        # Columns from the abstract model that map to storage columns present only
        # in the Events table
        self.__abstract_events_columns = abstract_events_columns
        self.__transactions_table = transactions_table
        # Columns from the abstract model that map to storage columns present only
        # in the Transactions table
        self.__abstract_transactions_columns = abstract_transactions_columns

        self.__event_translator = event_translator.concat(
            TranslationMappers(
                columns=[
                    ColumnToMapping(None, "release", None, "tags", "sentry:release"),
                    ColumnToMapping(None, "dist", None, "tags", "sentry:dist"),
                    ColumnToMapping(None, "user", None, "tags", "sentry:user"),
                    DefaultNoneColumnMapper(self.__abstract_transactions_columns),
                ]
            )
        )

        self.__transaction_translator = transaction_translator.concat(
            TranslationMappers(
                columns=[
                    ColumnToLiteral(None, "group_id", 0),
                    DefaultNoneColumnMapper(self.__abstract_events_columns),
                ]
            )
        )

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> StorageAndMappers:
        table = detect_table(
            query,
            self.__abstract_events_columns,
            self.__abstract_transactions_columns,
            True,
        )

        if table == TRANSACTIONS:
            return StorageAndMappers(
                self.__transactions_table, self.__transaction_translator
            )
        else:
            use_readonly_storage = (
                state.get_config("enable_events_readonly_table", False)
                and not request_settings.get_consistent()
            )
            return (
                StorageAndMappers(self.__events_ro_table, self.__event_translator)
                if use_readonly_storage
                else StorageAndMappers(self.__events_table, self.__event_translator)
            )


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

        events_storage = get_storage(StorageKey.EVENTS)
        events_ro_storage = get_storage(StorageKey.EVENTS_RO)
        transactions_storage = get_storage(StorageKey.TRANSACTIONS)

        super().__init__(
            storages=[events_storage, transactions_storage],
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=DiscoverQueryStorageSelector(
                    events_table=events_storage,
                    events_ro_table=events_ro_storage,
                    abstract_events_columns=self.__events_columns,
                    transactions_table=transactions_storage,
                    abstract_transactions_columns=self.__transactions_columns,
                ),
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
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            # Apdex and Impact seem very good candidates for
            # being defined by the Transaction entity when it will
            # exist, so it would run before Storage selection.
            ApdexProcessor(),
            ImpactProcessor(),
            FailureRateProcessor(),
            TimeSeriesColumnProcessor({"time": "timestamp"}),
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
            query, self.__events_columns, self.__transactions_columns, False,
        )

        if detected_dataset == TRANSACTIONS:
            if column_name == "group_id":
                # TODO: We return 0 here instead of NULL so conditions like group_id
                # in (1, 2, 3) will work, since Clickhouse won't run a query like:
                # SELECT (NULL AS group_id) FROM transactions WHERE group_id IN (1, 2, 3)
                # When we have the query AST, we should solve this by transforming the
                # nonsensical conditions instead.
                return "0"
            if self.__events_columns.get(column_name):
                return "NULL"
        else:
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
