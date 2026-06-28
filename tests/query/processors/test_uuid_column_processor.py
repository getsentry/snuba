import uuid

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Column,
    DangerousRawSQL,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.processors.physical.type_converters import ColumnTypeError
from snuba.query.processors.physical.uuid_column_processor import UUIDColumnProcessor
from snuba.query.query_settings import HTTPQuerySettings

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
        ),
        "equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
        id="equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "notauuid"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "notauuid"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        "equals(notauuid, 'a7d67cf796774551a95be6543cacd459')",
        id="notauuid, 'a7d67cf796774551a95be6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
            Column(None, None, "column1"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
        ),
        "equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
        id="equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(
                None,
                "tuple",
                (
                    Literal(None, "a7d67cf796774551a95be6543cacd459"),
                    Literal(None, "a7d67cf796774551a95be6543cacd45a"),
                ),
            ),
        ),
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(
                None,
                "tuple",
                (
                    Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
                    Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd45a"))),
                ),
            ),
        ),
        "in(column1, ('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
        id="in(column1, ('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
        ),
        "equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
        id="equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "column1"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.GTE,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(
                        None,
                        "toString",
                        (Column(None, None, "column1"),),
                    ),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        "greaterOrEquals(replaceAll(toString(column1), '-', ''), 'a7d67cf796774551a95be6543cacd459')",
        id="Use string UUID if GTE",
    ),
    pytest.param(
        binary_condition(
            BooleanFunctions.OR,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "column1"),
                Literal(None, "a7d67cf796774551a95be6543cacd458"),
            ),
            binary_condition(
                ConditionFunctions.GTE,
                Column(None, None, "column1"),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        binary_condition(
            BooleanFunctions.OR,
            binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "replaceAll",
                    (
                        FunctionCall(
                            None,
                            "toString",
                            (Column(None, None, "column1"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "a7d67cf796774551a95be6543cacd458"),
            ),
            binary_condition(
                ConditionFunctions.GTE,
                FunctionCall(
                    None,
                    "replaceAll",
                    (
                        FunctionCall(
                            None,
                            "toString",
                            (Column(None, None, "column1"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        "(equals(replaceAll(toString(column1), '-', ''), 'a7d67cf796774551a95be6543cacd458') OR greaterOrEquals(replaceAll(toString(column1), '-', ''), 'a7d67cf796774551a95be6543cacd459'))",
        id="Both optimizable and unoptimizable conditions in query",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "notauuid"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "notauuid"),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        "equals(notauuid, 'a7d67cf796774551a95be6543cacd459')",
        id="equals(notauuid, 'a7d67cf796774551a95be6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "column2"),
                Literal(None, "a7d67cf796774551a95be6543cacd460"),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "column1"),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "column2"),
                Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd460"))),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "column1"),
                Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
            ),
        ),
        "equals(column2, 'a7d67cf7-9677-4551-a95b-e6543cacd460') AND equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
        id="equals(column2, 'a7d67cf7-9677-4551-a95b-e6543cacd460') AND equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(None, "cast", (Column(None, None, "column1"), Literal(None, "String"))),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
        ),
        "equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
        id="equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            FunctionCall(None, "cast", (Column(None, None, "column1"), Literal(None, "String"))),
            FunctionCall(None, "tuple", (Literal(None, "a7d67cf796774551a95be6543cacd459"),)),
        ),
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(None, "tuple", (Literal(None, "a7d67cf7-9677-4551-a95b-e6543cacd459"),)),
        ),
        "in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459'))",
        id="in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459'))",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests)
def test_uuid_column_processor(
    unprocessed: Expression,
    expected: Expression,
    formatted_value: str,
) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    expected_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=expected,
    )

    UUIDColumnProcessor({"column1", "column2"}).process_query(
        unprocessed_query, HTTPQuerySettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column2",
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(
                        None,
                        "toString",
                        (Column(None, None, "column2"),),
                    ),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
        )
    ]

    assert expected_query.get_condition() == unprocessed_query.get_condition()
    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value


tests_invalid_uuid = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, "deadbeefabad"),
        ),
        id="invalid uuid",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(None, "tuple", (Literal(None, "deadbeefabad"),)),
        ),
        id="invalid uuid - IN condition",
    ),
]


@pytest.mark.parametrize("unprocessed", tests_invalid_uuid)
def test_invalid_uuid(unprocessed: Expression) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )

    with pytest.raises(ColumnTypeError):
        UUIDColumnProcessor({"column1", "column2"}).process_query(
            unprocessed_query, HTTPQuerySettings()
        )


def _query_with_condition(condition: Expression) -> Query:
    return Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=condition,
    )


def test_raw_sql_in_subquery_predicate_keeps_column_bare() -> None:
    """A ``column IN (<subquery>)`` predicate emitted as ``DangerousRawSQL`` must survive
    ``UUIDColumnProcessor`` untouched, so the bare column can use its bloom-filter skip
    index.

    The processor only keeps a UUID column bare for ``=``/``IN`` comparisons against
    literal values. An ``in(column, <subquery>)`` term (the RHS is not a literal
    array/tuple) is not recognized, so the processor falls back to wrapping the column in
    ``replaceAll(toString(column), '-', '')`` -- which defeats the index. RPC cross-item
    queries therefore build the whole predicate as raw SQL (see
    ``cross_item_queries.trace_id_in_subquery_condition``).
    """
    subquery = "SELECT column1 FROM transactions"

    # Fixed form: the entire predicate is raw SQL, so column1 stays bare and untouched.
    raw_predicate = DangerousRawSQL(None, f"column1 IN ({subquery})")
    raw_query = _query_with_condition(raw_predicate)
    UUIDColumnProcessor({"column1", "column2"}).process_query(raw_query, HTTPQuerySettings())
    assert raw_query.get_condition() == raw_predicate

    # Regression guard: the previous form wraps column1, hiding it from the skip index.
    wrapped_query = _query_with_condition(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            DangerousRawSQL(None, f"({subquery})"),
        )
    )
    UUIDColumnProcessor({"column1", "column2"}).process_query(
        wrapped_query, HTTPQuerySettings()
    )
    wrapped_condition = wrapped_query.get_condition()
    assert wrapped_condition is not None
    formatted = wrapped_condition.accept(ClickhouseExpressionFormatter())
    assert "replaceAll(toString(column1)" in formatted


def test_raw_sql_select_column_is_not_rewritten() -> None:
    """A ``DangerousRawSQL`` projection (used by the cross-item trace-id subquery so it
    emits a real ``UUID`` instead of ``replaceAll(toString(trace_id), '-', '')``) must be
    left untouched by ``UUIDColumnProcessor``."""
    query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[
            SelectedExpression("column1", DangerousRawSQL(None, "column1")),
        ],
        condition=None,
    )
    UUIDColumnProcessor({"column1", "column2"}).process_query(query, HTTPQuerySettings())
    assert query.get_selected_columns() == [
        SelectedExpression("column1", DangerousRawSQL(None, "column1"))
    ]
