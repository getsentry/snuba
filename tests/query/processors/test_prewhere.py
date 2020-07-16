from typing import Any, MutableMapping, Optional, Sequence

import pytest

from snuba import settings
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    not_in_condition,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.types import Condition
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        {"conditions": [[["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]]},
        [
            "event_id",
            "group_id",
            "tags[sentry:release]",
            "message",
            "environment",
            "project_id",
        ],
        [],
        None,
        [[["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]],
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["!="],
            (
                FunctionCall(
                    None,
                    "positionCaseInsensitive",
                    (Column("message", None, "message"), Literal(None, "abc")),
                ),
                Literal(None, 0),
            ),
        ),
    ),
    (
        # Add pre-where condition in the expected order
        {
            "conditions": [
                ["d", "=", "1"],
                ["c", "=", "3"],
                ["a", "=", "1"],
                ["b", "=", "2"],
            ],
        },
        ["a", "b", "c"],
        [["d", "=", "1"], ["c", "=", "3"]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("d", None, "d"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("c", None, "c"), Literal(None, "3")),
                ),
            ),
        ),
        [["a", "=", "1"], ["b", "=", "2"]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("a", None, "a"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("b", None, "b"), Literal(None, "2")),
                ),
            ),
        ),
    ),
    (
        # Do not add conditions that are parts of an OR
        {"conditions": [[["a", "=", "1"], ["b", "=", "2"]], ["c", "=", "3"]]},
        ["a", "b", "c"],
        [[["a", "=", "1"], ["b", "=", "2"]]],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("a", None, "a"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("b", None, "b"), Literal(None, "2")),
                ),
            ),
        ),
        [["c", "=", "3"]],
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("c", None, "c"), Literal(None, "3")),
        ),
    ),
    (
        # Exclude NOT IN condition from the prewhere as they are generally not excluding
        # most of the dataset.
        {"conditions": [["a", "NOT IN", [1, 2, 3]], ["b", "=", "2"], ["c", "=", "3"]]},
        ["a", "b"],
        [["a", "NOT IN", [1, 2, 3]], ["c", "=", "3"]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                not_in_condition(
                    None,
                    Column("a", None, "a"),
                    [Literal(None, 1), Literal(None, 2), Literal(None, 3)],
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("c", None, "c"), Literal(None, "3")),
                ),
            ),
        ),
        [["b", "=", "2"]],
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("b", None, "b"), Literal(None, "2")),
        ),
    ),
]


@pytest.mark.parametrize(
    "query_body, keys, new_conditions, new_ast_condition, prewhere_conditions, new_prewhere_ast_condition",
    test_data,
)
def test_prewhere(
    query_body: MutableMapping[str, Any],
    keys: Sequence[str],
    new_conditions: Sequence[Condition],
    new_ast_condition: Optional[Expression],
    prewhere_conditions: Sequence[Condition],
    new_prewhere_ast_condition: Optional[Expression],
) -> None:
    settings.MAX_PREWHERE_CONDITIONS = 2
    events = get_dataset("events")
    query = parse_query(query_body, events)
    query.set_data_source(TableSource("my_table", ColumnSet([]), None, keys))

    request_settings = HTTPRequestSettings()
    processor = PrewhereProcessor()
    processor.process_query(Query(query), request_settings)

    assert query.get_conditions() == new_conditions
    assert query.get_condition_from_ast() == new_ast_condition
    assert query.get_prewhere() == prewhere_conditions
    assert query.get_prewhere_ast() == new_prewhere_ast_condition
