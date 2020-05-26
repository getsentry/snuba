import pytest
import uuid

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.uuid_column_processor import UUIDColumnProcessor
from snuba.request.request_settings import HTTPRequestSettings


tests = [
    (
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                Column(None, "column1", None),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                Column(None, "column1", None),
                Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
            ),
        ),
        "(equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459') AS mightaswell)",
    ),
    (
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
                Column(None, "column1", None),
            ),
        ),
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
                Column(None, "column1", None),
            ),
        ),
        "(equals('a7d67cf7-9677-4551-a95b-e6543cacd459', column1) AS mightaswell)",
    ),
    (
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.IN,
                Column(None, "column1", None),
                FunctionCall(
                    None,
                    "tuple",
                    (
                        Literal(None, "a7d67cf796774551a95be6543cacd459"),
                        Literal(None, "a7d67cf796774551a95be6543cacd45a"),
                    ),
                ),
            ),
        ),
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.IN,
                Column(None, "column1", None),
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
        ),
        "(in(column1, tuple('a7d67cf7-9677-4551-a95b-e6543cacd459', 'a7d67cf7-9677-4551-a95b-e6543cacd45a')) AS mightaswell)",
    ),
    (
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "replaceAll",
                    (
                        FunctionCall(
                            None, "toString", (Column(None, "column1", None),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        Query(
            {},
            TableSource("transactions", ColumnSet([])),
            selected_columns=[Column(None, "column2", None)],
            condition=binary_condition(
                "mightaswell",
                ConditionFunctions.EQ,
                Column(None, "column1", None),
                Literal(None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))),
            ),
        ),
        "(equals(column1, 'a7d67cf7-9677-4551-a95b-e6543cacd459') AS mightaswell)",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests)
def test_uuid_column_processor(unprocessed, expected, formatted_value) -> None:
    UUIDColumnProcessor(set(["column1", "column2"])).process_query(
        unprocessed, HTTPRequestSettings()
    )
    assert expected.get_condition_from_ast() == unprocessed.get_condition_from_ast()

    ret = unprocessed.get_condition_from_ast().accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value
