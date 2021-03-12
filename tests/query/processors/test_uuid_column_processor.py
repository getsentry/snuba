import pytest
import uuid

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.clickhouse.query import Query
from snuba.query.processors.uuid_column_processor import (
    UUIDColumnProcessor,
    BetterUUIDColumnProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings


tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
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
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "notauuid"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "notauuid"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        "equals(replaceAll(toString(notauuid), '-', ''), 'a7d67cf796774551a95be6543cacd459')",
        id="equals(replaceAll(toString(notauuid), '-', ''), 'a7d67cf796774551a95be6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
            Column(None, None, "column1"),
        ),
        "equals('a7d67cf7-9677-4551-a95b-e6543cacd459', column1)",
        id="equals('a7d67cf7-9677-4551-a95b-e6543cacd459', column1)",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
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
                    Literal(
                        None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))
                    ),
                    Literal(
                        None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd45a"))
                    ),
                ),
            ),
        ),
        "in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
        id="in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
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
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "notauuid"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "notauuid"),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "a7d67cf796774551a95be6543cacd459"),
        ),
        "equals(replaceAll(toString(notauuid), '-', ''), 'a7d67cf796774551a95be6543cacd459')",
        id="equals(replaceAll(toString(notauuid), '-', ''), 'a7d67cf796774551a95be6543cacd459')",
    ),
    pytest.param(
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "replaceAll",
                    (
                        FunctionCall(
                            None, "toString", (Column(None, None, "column2"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "a7d67cf796774551a95be6543cacd460"),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "replaceAll",
                    (
                        FunctionCall(
                            None, "toString", (Column(None, None, "column1"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
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
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),)),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "deadbeefabad"),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column1"),)),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "deadbeefabad"),
        ),
        "equals(replaceAll(toString(column1), '-', ''), 'deadbeefabad')",
        id="equals(replaceAll(toString(column1), '-', ''), 'deadbeefabad')",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests)
def test_uuid_column_processor(
    unprocessed: Expression, expected: Expression, formatted_value: str,
) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    expected_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=expected,
    )

    UUIDColumnProcessor(set(["column1", "column2"])).process_query(
        unprocessed_query, HTTPRequestSettings()
    )
    assert expected_query.get_condition() == unprocessed_query.get_condition()
    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value


tests_new_processor = [
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
                    Literal(
                        None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))
                    ),
                    Literal(
                        None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd45a"))
                    ),
                ),
            ),
        ),
        "in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
        id="in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a'))",
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
            Column(None, None, "column1"),
            Literal(None, "deadbeefabad"),
        ),
        Literal(None, 0),
        "0",
        id="invalid uuid",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(None, "tuple", (Literal(None, "deadbeefabad"),)),
        ),
        Literal(None, 0),
        "0",
        id="invalid uuid - IN condition",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests_new_processor)
def test_better_uuid_column_processor(
    unprocessed: Expression, expected: Expression, formatted_value: str,
) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    expected_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=expected,
    )

    BetterUUIDColumnProcessor(set(["column1", "column2"])).process_query(
        unprocessed_query, HTTPRequestSettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column2",
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, None, "column2"),),),
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
