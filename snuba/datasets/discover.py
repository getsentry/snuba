import logging

from snuba import environment
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.discover import EVENTS_COLUMNS, TRANSACTIONS_COLUMNS
from snuba.query.conditions import (
    BINARY_OPERATORS,
    ConditionFunctions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or, Param
from snuba.query.matchers import String as StringMatch
from snuba.query.subscripts import subscript_key_column_name
from snuba.utils.metrics.wrapper import MetricsWrapper

EVENTS = EntityKey.DISCOVER_EVENTS
TRANSACTIONS = EntityKey.DISCOVER_TRANSACTIONS
EVENTS_AND_TRANSACTIONS = EntityKey.DISCOVER


class DiscoverDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.DISCOVER)

    # XXX: This is temporary code that will eventually need to be ported to Sentry
    # since SnQL will require an entity to always be specified by the user.
    def select_entity(self, query: Query) -> EntityKey:
        selected_entity = match_query_to_entity(
            query, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS
        )

        track_bad_query(query, selected_entity, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS)

        return selected_entity


metrics = MetricsWrapper(environment.metrics, "api.discover")
logger = logging.getLogger(__name__)


EVENT_CONDITION = FunctionCallMatch(
    Param("function", Or([StringMatch(op) for op in BINARY_OPERATORS])),
    (
        Or([ColumnMatch(None, StringMatch("type")), LiteralMatch(None)]),
        Param("event_type", Or([ColumnMatch(), LiteralMatch()])),
    ),
)

TRANSACTION_FUNCTIONS = FunctionCallMatch(
    Or([StringMatch("apdex"), StringMatch("failure_rate")]), None
)

EVENT_FUNCTIONS = FunctionCallMatch(
    Or([StringMatch("isHandled"), StringMatch("notHandled")]), None
)


def match_query_to_entity(
    query: Query, events_only_columns: ColumnSet, transactions_only_columns: ColumnSet,
) -> EntityKey:
    # First check for a top level condition on the event type
    condition = query.get_condition()
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

    # If we cannot clearly pick an entity from the top level conditions, then
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

    # Check for isHandled/notHandled
    if has_event_columns is False:
        for expr in query.get_all_expressions():
            match = EVENT_FUNCTIONS.match(expr)
            if match:
                has_event_columns = True

    # Check for apdex or failure rate
    if has_transaction_columns is False:
        for expr in query.get_all_expressions():
            match = TRANSACTION_FUNCTIONS.match(expr)
            if match:
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


def track_bad_query(
    query: Query,
    selected_entity: EntityKey,
    events_only_columns: ColumnSet,
    transactions_only_columns: ColumnSet,
) -> None:
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

    event_mismatch = event_columns and selected_entity == TRANSACTIONS
    transaction_mismatch = transaction_columns and selected_entity in [
        EVENTS,
        EVENTS_AND_TRANSACTIONS,
    ]

    if event_mismatch or transaction_mismatch:
        missing_columns = ",".join(
            sorted(event_columns if event_mismatch else transaction_columns)
        )
        selected_entity_str = (
            str(selected_entity.value)
            if isinstance(selected_entity, EntityKey)
            else selected_entity
        )

        metrics.increment(
            "query.impossible",
            tags={
                "selected_table": selected_entity_str,
                "missing_columns": missing_columns,
            },
        )
        logger.warning(
            "Discover generated impossible query",
            extra={
                "selected_table": selected_entity_str,
                "missing_columns": missing_columns,
            },
            exc_info=True,
        )

    if selected_entity == EVENTS_AND_TRANSACTIONS and (
        event_columns or transaction_columns
    ):
        # Not possible in future with merge table
        missing_events_columns = ",".join(sorted(event_columns))
        missing_transactions_columns = ",".join(sorted(transaction_columns))
        metrics.increment(
            "query.impossible-merge-table",
            tags={
                "missing_events_columns": missing_events_columns,
                "missing_transactions_columns": missing_transactions_columns,
            },
        )
        logger.warning(
            "Discover generated impossible query - merge table",
            extra={
                "missing_events_columns": missing_events_columns,
                "missing_transactions_columns": missing_transactions_columns,
            },
            exc_info=True,
        )

    else:
        metrics.increment("query.success")
