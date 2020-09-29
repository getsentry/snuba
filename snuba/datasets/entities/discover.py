import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Mapping, Optional, Sequence, Set, Tuple, Union

from snuba import environment, state
from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Float,
    LowCardinality,
    Nested,
    Nullable,
    String,
    UInt,
)
from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.mappers import ColumnToLiteral, ColumnToMapping
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.events import event_translator
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import (
    QueryStorageSelector,
    ReadableStorage,
    StorageAndMappers,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.entities.transactions import transaction_translator
from snuba.query.conditions import (
    BINARY_OPERATORS,
    ConditionFunctions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import Column, Literal, SubscriptableReference
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import String as StringMatch
from snuba.query.matchers import Or, Param
from snuba.query.processors import QueryProcessor
from snuba.query.processors.performance_expressions import (
    apdex_processor,
    failure_rate_processor,
)
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.subscripts import subscript_key_column_name
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.util import parse_datetime, qualified_column
from snuba.utils.metrics.wrapper import MetricsWrapper

EVENTS = EntityKey.EVENTS
TRANSACTIONS = EntityKey.TRANSACTIONS
EVENTS_AND_TRANSACTIONS = "events_and_transactions"

metrics = MetricsWrapper(environment.metrics, "api.discover")
logger = logging.getLogger(__name__)

EVENT_CONDITION = FunctionCallMatch(
    Param("function", Or([StringMatch(op) for op in BINARY_OPERATORS])),
    (
        Or([ColumnMatch(None, StringMatch("type")), LiteralMatch(None)]),
        Param("event_type", Or([ColumnMatch(), LiteralMatch()])),
    ),
)


def match_query_to_table(
    query: Query, events_only_columns: ColumnSet, transactions_only_columns: ColumnSet
) -> Union[EntityKey, str]:
    # First check for a top level condition on the event type
    condition = query.get_condition_from_ast()
    event_types = set()
    if condition:
        top_level_condition = get_first_level_and_conditions(condition)

        for cond in top_level_condition:
            result = EVENT_CONDITION.match(cond)
            if not result:
                continue

            event_type_param = result.expression("event_type")

            if isinstance(event_type_param, Column):
                event_type = event_type_param.column_name
            elif isinstance(event_type_param, Literal):
                event_type = str(event_type_param.value)
            if result:
                if result.string("function") == ConditionFunctions.EQ:
                    event_types.add(event_type)
                elif result.string("function") == ConditionFunctions.NEQ:
                    if event_type == "transaction":
                        return EVENTS

    if len(event_types) == 1 and "transaction" in event_types:
        return TRANSACTIONS

    if len(event_types) > 0 and "transaction" not in event_types:
        return EVENTS

    # If we cannot clearly pick a table from the top level conditions, then
    # inspect the columns requested to infer a selection.
    has_event_columns = False
    has_transaction_columns = False
    for col in query.get_all_ast_referenced_columns():
        if events_only_columns.get(col.column_name):
            has_event_columns = True
        elif transactions_only_columns.get(col.column_name):
            has_transaction_columns = True

    for subscript in query.get_all_ast_referenced_subscripts():
        # Subscriptable references will not be properly recognized above
        # through get_all_ast_referenced_columns since the columns that
        # method will find will look like `tags` or `measurements`, while
        # the column sets contains `tags.key` and `tags.value`.
        schema_col_name = subscript_key_column_name(subscript)
        if events_only_columns.get(schema_col_name):
            has_event_columns = True
        if transactions_only_columns.get(schema_col_name):
            has_transaction_columns = True

    if has_event_columns and has_transaction_columns:
        # Impossible query, use the merge table
        return EVENTS_AND_TRANSACTIONS
    elif has_event_columns:
        return EVENTS
    elif has_transaction_columns:
        return TRANSACTIONS
    else:
        return EVENTS_AND_TRANSACTIONS


def detect_table(
    query: Query,
    events_only_columns: ColumnSet,
    transactions_only_columns: ColumnSet,
    track_bad_queries: bool,
) -> EntityKey:
    """
    Given a query, we attempt to guess whether it is better to fetch data from the
    "events", "transactions" or future merged storage.

    The merged storage resolves to the events storage until errors and transactions
    are split into separate physical tables.
    """
    selected_table = match_query_to_table(
        query, events_only_columns, transactions_only_columns
    )

    if track_bad_queries:
        event_columns = set()
        transaction_columns = set()
        for col in query.get_all_ast_referenced_columns():
            if events_only_columns.get(col.column_name):
                event_columns.add(col.column_name)
            elif transactions_only_columns.get(col.column_name):
                transaction_columns.add(col.column_name)

        for subscript in query.get_all_ast_referenced_subscripts():
            schema_col_name = subscript_key_column_name(subscript)
            if events_only_columns.get(schema_col_name):
                event_columns.add(schema_col_name)
            if transactions_only_columns.get(schema_col_name):
                transaction_columns.add(schema_col_name)

        event_mismatch = event_columns and selected_table == TRANSACTIONS
        transaction_mismatch = transaction_columns and selected_table in [
            EVENTS,
            EVENTS_AND_TRANSACTIONS,
        ]

        if event_mismatch or transaction_mismatch:
            missing_columns = ",".join(
                sorted(event_columns if event_mismatch else transaction_columns)
            )
            metrics.increment(
                "query.impossible",
                tags={
                    "selected_table": (
                        str(selected_table.value)
                        if isinstance(selected_table, EntityKey)
                        else selected_table
                    ),
                    "missing_columns": missing_columns,
                },
            )
            logger.warning("Discover generated impossible query", exc_info=True)

        if selected_table == EVENTS_AND_TRANSACTIONS and (
            event_columns or transaction_columns
        ):
            # Not possible in future with merge table
            metrics.increment(
                "query.impossible-merge-table",
                tags={
                    "missing_events_columns": ",".join(sorted(event_columns)),
                    "missing_transactions_columns": ",".join(
                        sorted(transaction_columns)
                    ),
                },
            )

        else:
            metrics.increment("query.success")

    # Default for events and transactions is events
    final_table = (
        EntityKey.EVENTS if selected_table != TRANSACTIONS else EntityKey.TRANSACTIONS
    )
    return final_table


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


@dataclass(frozen=True)
class DefaultNoneSubscriptMapper(SubscriptableReferenceMapper):
    """
    This maps a subscriptable reference to None (NULL in SQL) as it is done
    in the discover column_expr method today. It should not be used for
    any other reason or use case, thus it should not be moved out of
    the discover dataset file.
    """

    subscript_names: Set[str]

    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Literal]:
        if expression.column.column_name in self.subscript_names:
            return Literal(alias=expression.alias, value=None)
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
                ],
                subscriptables=[DefaultNoneSubscriptMapper({"measurements"})],
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


class DiscoverEntity(Entity):
    """
    Entity for the Discover product that maps the columns of Events and
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
                ("http_method", Nullable(String())),
                ("http_referer", Nullable(String())),
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
                (
                    "measurements",
                    Nested([("key", LowCardinality(String())), ("value", Float(64))]),
                ),
            ]
        )

        events_storage = get_storage(StorageKey.EVENTS)
        events_ro_storage = get_storage(StorageKey.EVENTS_RO)
        transactions_storage = get_storage(StorageKey.TRANSACTIONS)

        self.__time_group_columns: Mapping[str, str] = {}
        self.__time_parse_columns = ("timestamp",)

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
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        columnset = self.get_data_model()
        return [
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            # Apdex and Impact seem very good candidates for
            # being defined by the Transaction entity when it will
            # exist, so it would run before Storage selection.
            apdex_processor(columnset),
            failure_rate_processor(columnset),
            HandledFunctionsProcessor("exception_stacks.mechanism_handled", columnset),
            TimeSeriesColumnProcessor({"time": "timestamp"}),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    # TODO: This needs to burned with fire, for so many reasons.
    # It's here now to reduce the scope of the initial entity changes
    # but can be moved to a processor if not removed entirely.
    def process_condition(
        self, condition: Tuple[str, str, Any]
    ) -> Tuple[str, str, Any]:
        lhs, op, lit = condition
        if (
            lhs in self.__time_parse_columns
            and op in (">", "<", ">=", "<=", "=", "!=")
            and isinstance(lit, str)
        ):
            lit = parse_datetime(lit)
        return lhs, op, lit
