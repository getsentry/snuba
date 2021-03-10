from typing import Any, MutableMapping, Optional, Sequence

import pytest
from snuba import settings
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.datasets.storages.errors_common import all_columns
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    not_in_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.processors.prewhere import PrewhereProcessor
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
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["!="],
            (
                FunctionCall(
                    None,
                    "positionCaseInsensitive",
                    (Column("_snuba_message", None, "message"), Literal(None, "abc")),
                ),
                Literal(None, 0),
            ),
        ),
        False,
    ),
    (
        # Add pre-where condition in the expected order
        {
            "conditions": [
                ["d", "=", "1"],
                ["c", "=", "3"],
                [["and", [["equals", ["a", "'1'"]], ["equals", ["b", "'2'"]]]], "=", 1],
            ],
        },
        ["a", "b", "c"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_d", None, "d"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_c", None, "c"), Literal(None, "3")),
                ),
            ),
        ),
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_a", None, "a"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_b", None, "b"), Literal(None, "2")),
                ),
            ),
        ),
        False,
    ),
    (
        # Do not add conditions that are parts of an OR
        {"conditions": [[["a", "=", "1"], ["b", "=", "2"]], ["c", "=", "3"]]},
        ["a", "b", "c"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_a", None, "a"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_b", None, "b"), Literal(None, "2")),
                ),
            ),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_c", None, "c"), Literal(None, "3")),
        ),
        False,
    ),
    (
        # Exclude NOT IN condition from the prewhere as they are generally not excluding
        # most of the dataset.
        {"conditions": [["a", "NOT IN", [1, 2, 3]], ["b", "=", "2"], ["c", "=", "3"]]},
        ["a", "b"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                not_in_condition(
                    Column("_snuba_a", None, "a"),
                    [Literal(None, 1), Literal(None, 2), Literal(None, 3)],
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_c", None, "c"), Literal(None, "3")),
                ),
            ),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_b", None, "b"), Literal(None, "2")),
        ),
        False,
    ),
    # Does not promote omit_if_final columns
    (
        {"conditions": [["environment", "=", "abc"], ["project_id", "=", 1]]},
        [
            "event_id",
            "group_id",
            "tags[sentry:release]",
            "message",
            "environment",
            "project_id",
        ],
        ["environment"],
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_environment", None, "environment"), Literal(None, "abc"),),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_project_id", None, "project_id"), Literal(None, 1),),
        ),
        True,
    ),
]


@pytest.mark.parametrize(
    "query_body, keys, omit_if_final_keys, new_ast_condition, new_prewhere_ast_condition, final",
    test_data,
)
def test_prewhere(
    query_body: MutableMapping[str, Any],
    keys: Sequence[str],
    omit_if_final_keys: Sequence[str],
    new_ast_condition: Optional[Expression],
    new_prewhere_ast_condition: Optional[Expression],
    final: bool,
) -> None:
    settings.MAX_PREWHERE_CONDITIONS = 2
    events = get_dataset("events")
    query = identity_translate(parse_query(query_body, events))
    query.set_from_clause(Table("my_table", all_columns, final=final))

    request_settings = HTTPRequestSettings()
    processor = PrewhereProcessor(keys, omit_if_final=omit_if_final_keys)
    processor.process_query(query, request_settings)

    assert query.get_condition() == new_ast_condition
    assert query.get_prewhere_ast() == new_prewhere_ast_condition
