from typing import Any, MutableMapping, Optional, Sequence

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba import settings
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    get_first_level_and_conditions,
    not_in_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.physical.prewhere import PrewhereProcessor
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query

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
                ["event_id", "=", "1"],
                ["partition", "=", "3"],
                [
                    [
                        "and",
                        [
                            ["equals", ["offset", "'1'"]],
                            ["equals", ["retention_days", "'2'"]],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
        },
        ["offset", "retention_days", "partition"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (
                        Column("_snuba_event_id", None, "event_id"),
                        Literal(None, "1"),
                    ),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_partition", None, "partition"), Literal(None, "3")),
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
                    (Column("_snuba_offset", None, "offset"), Literal(None, "1")),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (
                        Column("_snuba_retention_days", None, "retention_days"),
                        Literal(None, "2"),
                    ),
                ),
            ),
        ),
        False,
    ),
    (
        # Do not add conditions that are parts of an OR
        {
            "conditions": [
                [["project_id", "=", "1"], ["partition", "=", "2"]],
                ["offset", "=", "3"],
            ]
        },
        ["project_id", "partition", "offset"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        Literal(None, "1"),
                    ),
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_partition", None, "partition"), Literal(None, "2")),
                ),
            ),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_offset", None, "offset"), Literal(None, "3")),
        ),
        False,
    ),
    (
        # Exclude NOT IN condition from the prewhere as they are generally not excluding
        # most of the dataset.
        {
            "conditions": [
                ["event_id", "NOT IN", [1, 2, 3]],
                ["partition", "=", "2"],
                ["offset", "=", "3"],
            ]
        },
        ["event_id", "partition"],
        [],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                not_in_condition(
                    Column("_snuba_event_id", None, "event_id"),
                    [Literal(None, 1), Literal(None, 2), Literal(None, 3)],
                ),
                FunctionCall(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    (Column("_snuba_offset", None, "offset"), Literal(None, "3")),
                ),
            ),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (Column("_snuba_partition", None, "partition"), Literal(None, "2")),
        ),
        False,
    ),
    # Does not promote omit_if_final columns
    (
        {"conditions": [["environment", "=", "abc"]]},
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
            (
                Column("_snuba_environment", None, "environment"),
                Literal(None, "abc"),
            ),
        ),
        FunctionCall(
            None,
            OPERATOR_TO_FUNCTION["="],
            (
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
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
    # HACK until we migrate these tests to SnQL
    query_body["selected_columns"] = ["project_id"]
    query_body["conditions"] += [
        ["timestamp", ">=", "2021-01-01T00:00:00"],
        ["timestamp", "<", "2021-01-02T00:00:00"],
        ["project_id", "=", 1],
    ]
    request = json_to_snql(query_body, "events")
    request.validate()
    snql_query, _ = parse_snql_query(str(request.query), events)
    assert isinstance(snql_query, Query)
    query = identity_translate(snql_query)

    all_columns = get_storage(StorageKey.ERRORS).get_schema().get_columns()
    query.set_from_clause(Table("my_table", all_columns, final=final))

    query_settings = HTTPQuerySettings()
    processor = PrewhereProcessor(keys, omit_if_final=omit_if_final_keys)
    processor.process_query(query, query_settings)

    # HACK until we migrate these tests to SnQL
    def verify_expressions(top_level: Expression, expected: Expression) -> bool:
        actual_conds = get_first_level_and_conditions(top_level)
        expected_conds = get_first_level_and_conditions(expected)
        for cond in expected_conds:
            if cond not in actual_conds:
                return False

        return True

    if new_ast_condition:
        condition = query.get_condition()
        assert condition is not None
        assert verify_expressions(condition, new_ast_condition)

    if new_prewhere_ast_condition:
        prewhere = query.get_prewhere_ast()
        assert prewhere is not None
        assert verify_expressions(prewhere, new_prewhere_ast_condition)
