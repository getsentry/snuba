import logging

from snuba import environment
from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Float,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.datasets.entities.entity_key import EntityKey
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

"""

The Discover dataset is a special exception in many ways. This is one of them.

A discover query is matched to a certain entity based on the properties of the query.
This should not generally be the case so it is not abstracted in the Dataset logic but
is contained within this file to be used by the parser itself.

XXX: This is temporary code that will eventually need to be ported to Sentry since SnQL
will require an entity to always be specified by the user.

"""


# NOTE: These will need to be updated if/when events and transaction columns change.
# TODO: Figure out a way to get this information automatically.
EVENTS_COLUMNS = ColumnSet(
    [
        ("group_id", UInt(64, Modifiers(nullable=True))),
        ("primary_hash", FixedString(32, Modifiers(nullable=True))),
        # Promoted tags
        ("level", String(Modifiers(nullable=True))),
        ("logger", String(Modifiers(nullable=True))),
        ("server_name", String(Modifiers(nullable=True))),
        ("site", String(Modifiers(nullable=True))),
        ("url", String(Modifiers(nullable=True))),
        ("location", String(Modifiers(nullable=True))),
        ("culprit", String(Modifiers(nullable=True))),
        ("received", DateTime(Modifiers(nullable=True))),
        ("sdk_integrations", Array(String(), Modifiers(nullable=True))),
        ("version", String(Modifiers(nullable=True))),
        # exception interface
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String(Modifiers(nullable=True))),
                    ("value", String(Modifiers(nullable=True))),
                    ("mechanism_type", String(Modifiers(nullable=True))),
                    ("mechanism_handled", UInt(8, Modifiers(nullable=True))),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String(Modifiers(nullable=True))),
                    ("filename", String(Modifiers(nullable=True))),
                    ("package", String(Modifiers(nullable=True))),
                    ("module", String(Modifiers(nullable=True))),
                    ("function", String(Modifiers(nullable=True))),
                    ("in_app", UInt(8, Modifiers(nullable=True))),
                    ("colno", UInt(32, Modifiers(nullable=True))),
                    ("lineno", UInt(32, Modifiers(nullable=True))),
                    ("stack_level", UInt(16)),
                ]
            ),
        ),
        ("modules", Nested([("name", String()), ("version", String())])),
        ("exception_main_thread", UInt(8, Modifiers(nullable=True))),
        ("trace_sampled", UInt(8, Modifiers(nullable=True))),
        ("num_processing_errors", UInt(64, Modifiers(nullable=True))),
    ]
)

TRANSACTIONS_COLUMNS = ColumnSet(
    [
        ("transaction_hash", UInt(64, Modifiers(nullable=True))),
        ("transaction_op", String(Modifiers(nullable=True))),
        ("transaction_status", UInt(8, Modifiers(nullable=True))),
        ("transaction_source", String(Modifiers(nullable=True))),
        ("duration", UInt(32, Modifiers(nullable=True))),
        ("measurements", Nested([("key", String()), ("value", Float(64))])),
        ("span_op_breakdowns", Nested([("key", String()), ("value", Float(64))])),
        (
            "spans",
            Nested(
                [
                    ("op", String()),
                    ("group", UInt(64)),
                    ("exclusive_time", Float(64)),
                    ("exclusive_time_32", Float(32)),
                ]
            ),
        ),
        ("group_ids", Array(UInt(64, Modifiers(nullable=True)))),
        ("app_start_type", String(Modifiers(nullable=True))),
        ("profile_id", UUID(Modifiers(nullable=True))),
    ]
)


def select_discover_entity(query: Query) -> EntityKey:
    selected_entity = match_query_to_entity(query, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS)

    _track_bad_query(query, selected_entity, EVENTS_COLUMNS, TRANSACTIONS_COLUMNS)

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
    query: Query,
    events_only_columns: ColumnSet,
    transactions_only_columns: ColumnSet,
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
                        return EntityKey.DISCOVER_EVENTS

    if len(event_types) == 1 and "transaction" in event_types:
        return EntityKey.DISCOVER_TRANSACTIONS

    if len(event_types) > 0 and "transaction" not in event_types:
        return EntityKey.DISCOVER_EVENTS

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
        return EntityKey.DISCOVER
    elif has_event_columns:
        return EntityKey.DISCOVER_EVENTS
    elif has_transaction_columns:
        return EntityKey.DISCOVER_TRANSACTIONS
    else:
        return EntityKey.DISCOVER


def _track_bad_query(
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

    event_mismatch = (
        event_columns and selected_entity == EntityKey.DISCOVER_TRANSACTIONS
    )
    transaction_mismatch = transaction_columns and selected_entity in [
        EntityKey.DISCOVER_EVENTS,
        EntityKey.DISCOVER,
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

    if selected_entity == EntityKey.DISCOVER and (event_columns or transaction_columns):
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

    else:
        metrics.increment("query.success")
