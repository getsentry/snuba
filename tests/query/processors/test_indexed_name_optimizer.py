from copy import deepcopy

import pytest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import (
    and_cond,
    arrayElement,
    column,
    equals,
    in_cond,
    literal,
    literals_array,
)
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical.indexed_name_optimizer import (
    IndexedNameOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import set_config

# Use the protobuf enum values directly so the tests stay in sync with the
# production dependency the optimizer keys off of.
SPAN = TraceItemType.TRACE_ITEM_TYPE_SPAN
METRIC = TraceItemType.TRACE_ITEM_TYPE_METRIC
LOG = TraceItemType.TRACE_ITEM_TYPE_LOG


def _attr_arr(key: str) -> FunctionCall:
    """The ``arrayElement(attributes_string, key)`` form the EAP RPC builder
    emits for a map-backed string filter (see ``_map_backed_operands``)."""
    return arrayElement(None, column("attributes_string"), literal(key))


def _attr_str(key: str, alias: str | None = None) -> SubscriptableReference:
    """The hand-written SnQL ``attributes_string[key]`` form."""
    return SubscriptableReference(
        alias=alias,
        column=column("attributes_string"),
        key=literal(key),
    )


def _query(
    condition: Expression,
    selected_columns: list[SelectedExpression] | None = None,
) -> Query:
    return Query(
        QueryEntity(EntityKey.EAP_ITEMS, ColumnSet([])),
        selected_columns=selected_columns or [SelectedExpression("c", column("project_id"))],
        condition=condition,
    )


test_data = [
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_arr("sentry.op"), literal("db.query")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(column("indexed_name"), literal("db.query")),
            )
        ),
        id="span sentry.op (arrayElement) rewritten to indexed_name",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_str("sentry.op"), literal("db.query")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(column("indexed_name"), literal("db.query")),
            )
        ),
        id="span sentry.op (SubscriptableReference) rewritten to indexed_name",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(_attr_arr("sentry.metric.name"), literal("my.metric")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(column("indexed_name"), literal("my.metric")),
            )
        ),
        id="metric sentry.metric.name rewritten to indexed_name",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                in_cond(
                    _attr_arr("sentry.op"),
                    literals_array(None, [literal("db.query"), literal("http.client")]),
                ),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                in_cond(
                    column("indexed_name"),
                    literals_array(None, [literal("db.query"), literal("http.client")]),
                ),
            )
        ),
        id="span sentry.op IN rewritten to indexed_name",
    ),
    pytest.param(
        # The rewrite is a value substitution, so it applies to any operator the
        # access appears under (the index only helps equals/in, but substituting
        # is always correct once indexed_name is populated).
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                FunctionCall(None, "notEquals", (_attr_arr("sentry.op"), literal("db.query"))),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                FunctionCall(None, "notEquals", (column("indexed_name"), literal("db.query"))),
            )
        ),
        id="any operator on the access is rewritten (notEquals)",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_arr("sentry.metric.name"), literal("my.metric")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_arr("sentry.metric.name"), literal("my.metric")),
            )
        ),
        id="span query with metric key is left untouched",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_arr("foo"), literal("bar")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_arr("foo"), literal("bar")),
            )
        ),
        id="non-indexed attribute is left untouched",
    ),
    pytest.param(
        _query(equals(_attr_arr("sentry.op"), literal("db.query"))),
        _query(equals(_attr_arr("sentry.op"), literal("db.query"))),
        id="no item_type condition leaves bucket lookup",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(LOG)),
                equals(_attr_arr("sentry.op"), literal("db.query")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(LOG)),
                equals(_attr_arr("sentry.op"), literal("db.query")),
            )
        ),
        id="unsupported item_type leaves bucket lookup",
    ),
    pytest.param(
        _query(
            condition=equals(column("item_type"), literal(SPAN)),
            selected_columns=[
                SelectedExpression("op", _attr_str("sentry.op", alias="op")),
            ],
        ),
        _query(
            condition=equals(column("item_type"), literal(SPAN)),
            selected_columns=[
                SelectedExpression("op", Column("op", None, "indexed_name")),
            ],
        ),
        id="select preserves alias on rewrite",
    ),
]


@pytest.mark.redis_db
@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_indexed_name_optimizer(pre_format: Query, expected_query: Query) -> None:
    set_config(IndexedNameOptimizer.CONFIG_KEY, 1)
    copy = deepcopy(pre_format)
    IndexedNameOptimizer().process_query(copy, HTTPQuerySettings())
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_condition() == expected_query.get_condition()


@pytest.mark.redis_db
def test_disabled_by_default_leaves_query_untouched() -> None:
    set_config(IndexedNameOptimizer.CONFIG_KEY, 0)
    query = _query(
        and_cond(
            equals(column("item_type"), literal(SPAN)),
            equals(_attr_arr("sentry.op"), literal("db.query")),
        )
    )
    copy = deepcopy(query)
    IndexedNameOptimizer().process_query(copy, HTTPQuerySettings())
    assert copy.get_condition() == query.get_condition()
    assert copy.get_selected_columns() == query.get_selected_columns()


@pytest.mark.redis_db
def test_contradictory_item_types_left_untouched() -> None:
    set_config(IndexedNameOptimizer.CONFIG_KEY, 1)
    query = _query(
        and_cond(
            equals(column("item_type"), literal(SPAN)),
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(_attr_arr("sentry.op"), literal("db.query")),
            ),
        )
    )
    copy = deepcopy(query)
    IndexedNameOptimizer().process_query(copy, HTTPQuerySettings())
    # ambiguous item_type -> no rewrite; no indexed_name reference is introduced.
    condition = copy.get_condition()
    assert condition is not None
    assert not any(isinstance(e, Column) and e.column_name == "indexed_name" for e in condition)
    # the original bucket lookup survives untouched.
    assert any(
        isinstance(e, FunctionCall)
        and e.function_name == "arrayElement"
        and isinstance(e.parameters[1], Literal)
        and e.parameters[1].value == "sentry.op"
        for e in condition
    )
