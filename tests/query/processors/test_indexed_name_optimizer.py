from copy import deepcopy

import pytest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.expressions import (
    Column,
    Expression,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical.indexed_name_optimizer import (
    IndexedNameOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings

# Use the protobuf enum values directly so the tests stay in sync with the
# production dependency the optimizer keys off of.
SPAN = TraceItemType.TRACE_ITEM_TYPE_SPAN
METRIC = TraceItemType.TRACE_ITEM_TYPE_METRIC
LOG = TraceItemType.TRACE_ITEM_TYPE_LOG


def _attr_str(key: str, alias: str | None = None) -> SubscriptableReference:
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
                equals(_attr_str("sentry.op"), literal("db.query")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(Column(None, None, "indexed_name"), literal("db.query")),
            )
        ),
        id="span sentry.op rewritten to indexed_name",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(_attr_str("sentry.metric.name"), literal("my.metric")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(Column(None, None, "indexed_name"), literal("my.metric")),
            )
        ),
        id="metric sentry.metric.name rewritten to indexed_name",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_str("sentry.metric.name"), literal("my.metric")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_str("sentry.metric.name"), literal("my.metric")),
            )
        ),
        id="span query with metric key is left untouched",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_str("foo"), literal("bar")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(SPAN)),
                equals(_attr_str("foo"), literal("bar")),
            )
        ),
        id="non-indexed attribute is left untouched",
    ),
    pytest.param(
        _query(equals(_attr_str("sentry.op"), literal("db.query"))),
        _query(equals(_attr_str("sentry.op"), literal("db.query"))),
        id="no item_type condition leaves bucket lookup",
    ),
    pytest.param(
        _query(
            and_cond(
                equals(column("item_type"), literal(LOG)),
                equals(_attr_str("sentry.op"), literal("db.query")),
            )
        ),
        _query(
            and_cond(
                equals(column("item_type"), literal(LOG)),
                equals(_attr_str("sentry.op"), literal("db.query")),
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


@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_indexed_name_optimizer(pre_format: Query, expected_query: Query) -> None:
    copy = deepcopy(pre_format)
    IndexedNameOptimizer().process_query(copy, HTTPQuerySettings())
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_condition() == expected_query.get_condition()


def test_contradictory_item_types_left_untouched() -> None:
    query = _query(
        and_cond(
            equals(column("item_type"), literal(SPAN)),
            and_cond(
                equals(column("item_type"), literal(METRIC)),
                equals(_attr_str("sentry.op"), literal("db.query")),
            ),
        )
    )
    copy = deepcopy(query)
    IndexedNameOptimizer().process_query(copy, HTTPQuerySettings())
    # ambiguous item_type -> no rewrite; the subscriptable access survives
    condition = copy.get_condition()
    assert condition is not None
    assert any(
        isinstance(e, SubscriptableReference)
        and isinstance(e.key, Literal)
        and e.key.value == "sentry.op"
        for e in condition
    )
