from typing import Generator

import pytest

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.joins.classifier import (
    BranchCutter,
    MainQueryExpression,
    SubExpression,
    SubqueryExpression,
    UnclassifiedExpression,
)

TEST_CASES = [
    pytest.param(
        Column("_snuba_col", "events", "column"),
        SubqueryExpression(Column("_snuba_col", None, "column"), "events"),
        MainQueryExpression(
            Column("_snuba_col", "events", "_snuba_col"),
            cut_branches={"events": {Column("_snuba_col", None, "column")}},
        ),
        id="Basic Column to push down",
    ),
    pytest.param(
        Column(None, "events", "column"),
        SubqueryExpression(Column(None, None, "column"), "events"),
        MainQueryExpression(
            Column("_snuba_gen_1", "events", "_snuba_gen_1"),
            cut_branches={"events": {Column("_snuba_gen_1", None, "column")}},
        ),
        id="Basic column with alias generation during branch cut",
    ),
    pytest.param(
        Literal(None, "value"),
        UnclassifiedExpression(Literal(None, "value")),
        MainQueryExpression(Literal(None, "value"), {}),
        id="Basic literal",
    ),
    pytest.param(
        FunctionCall(None, "f", (Literal(None, "asd"), Literal(None, "sdv"))),
        UnclassifiedExpression(
            FunctionCall(None, "f", (Literal(None, "asd"), Literal(None, "sdv")))
        ),
        MainQueryExpression(
            FunctionCall(None, "f", (Literal(None, "asd"), Literal(None, "sdv"))), {}
        ),
        id="Unclassified function",
    ),
    pytest.param(
        SubscriptableReference(
            "_snuba_tag", Column("_snuba_col", "events", "tags"), Literal(None, "val")
        ),
        SubqueryExpression(
            SubscriptableReference(
                "_snuba_tag", Column("_snuba_col", None, "tags"), Literal(None, "val")
            ),
            "events",
        ),
        MainQueryExpression(
            Column("_snuba_tag", "events", "_snuba_tag"),
            {
                "events": {
                    SubscriptableReference(
                        "_snuba_tag",
                        Column("_snuba_col", None, "tags"),
                        Literal(None, "val"),
                    ),
                }
            },
        ),
        id="Subscriptable reference",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (Column("_snuba_col", "events", "column"), Literal(None, "sdv")),
        ),
        SubqueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (Column("_snuba_col", None, "column"), Literal(None, "sdv")),
            ),
            subquery_alias="events",
        ),
        MainQueryExpression(
            Column("_snuba_f", "events", "_snuba_f"),
            {
                "events": {
                    FunctionCall(
                        "_snuba_f",
                        "f",
                        (Column("_snuba_col", None, "column"), Literal(None, "sdv")),
                    ),
                }
            },
        ),
        id="Function that fits in one subquery",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (
                Column("_snuba_col", "events", "column"),
                Column("_snuba_col2", "events", "column2"),
            ),
        ),
        SubqueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_col", None, "column"),
                    Column("_snuba_col2", None, "column2"),
                ),
            ),
            subquery_alias="events",
        ),
        MainQueryExpression(
            Column("_snuba_f", "events", "_snuba_f"),
            {
                "events": {
                    FunctionCall(
                        "_snuba_f",
                        "f",
                        (
                            Column("_snuba_col", None, "column"),
                            Column("_snuba_col2", None, "column2"),
                        ),
                    ),
                }
            },
        ),
        id="Function that fits in one subquery with two columns",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (
                Column("_snuba_col", "events", "column"),
                Column("_snuba_col2", "groups", "column2"),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                ),
            ),
            cut_branches={
                "events": {Column("_snuba_col", None, "column")},
                "groups": {Column("_snuba_col2", None, "column2")},
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                ),
            ),
            cut_branches={
                "events": {Column("_snuba_col", None, "column")},
                "groups": {Column("_snuba_col2", None, "column2")},
            },
        ),
        id="Cut a branch with simple expressions",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (
                Column("_snuba_col", "events", "column"),
                Column("_snuba_col2", "groups", "column2"),
                Column("_snuba_col3", "groups", "column3"),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                    Column("_snuba_col3", "groups", "_snuba_col3"),
                ),
            ),
            cut_branches={
                "events": {Column("_snuba_col", None, "column")},
                "groups": {
                    Column("_snuba_col2", None, "column2"),
                    Column("_snuba_col3", None, "column3"),
                },
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                    Column("_snuba_col3", "groups", "_snuba_col3"),
                ),
            ),
            cut_branches={
                "events": {Column("_snuba_col", None, "column")},
                "groups": {
                    Column("_snuba_col2", None, "column2"),
                    Column("_snuba_col3", None, "column3"),
                },
            },
        ),
        id="Ensure one of the entities has multiple independent branches.",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (
                FunctionCall(
                    "_snuba_g",
                    "g",
                    (Column("_snuba_col", "events", "column"), Literal(None, "val"),),
                ),
                Column("_snuba_col2", "groups", "column2"),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_g", "events", "_snuba_g"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                ),
            ),
            cut_branches={
                "events": {
                    FunctionCall(
                        "_snuba_g",
                        "g",
                        (Column("_snuba_col", None, "column"), Literal(None, "val"),),
                    )
                },
                "groups": {Column("_snuba_col2", None, "column2")},
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_g", "events", "_snuba_g"),
                    Column("_snuba_col2", "groups", "_snuba_col2"),
                ),
            ),
            cut_branches={
                "events": {
                    FunctionCall(
                        "_snuba_g",
                        "g",
                        (Column("_snuba_col", None, "column"), Literal(None, "val"),),
                    )
                },
                "groups": {Column("_snuba_col2", None, "column2")},
            },
        ),
        id="Nested expression, part pushed down",
    ),
    pytest.param(
        CurriedFunctionCall(
            "_snuba_cf",
            FunctionCall("_snuba_f", "f", (Column("_snuba_col", "events", "column"),)),
            (Literal(None, "val"),),
        ),
        SubqueryExpression(
            CurriedFunctionCall(
                "_snuba_cf",
                FunctionCall("_snuba_f", "f", (Column("_snuba_col", None, "column"),)),
                (Literal(None, "val"),),
            ),
            subquery_alias="events",
        ),
        MainQueryExpression(
            Column("_snuba_cf", "events", "_snuba_cf"),
            {
                "events": {
                    CurriedFunctionCall(
                        "_snuba_cf",
                        FunctionCall(
                            "_snuba_f", "f", (Column("_snuba_col", None, "column"),)
                        ),
                        (Literal(None, "val"),),
                    ),
                },
            },
        ),
        id="Curried Function",
    ),
    pytest.param(
        FunctionCall("_snuba_f", "avg", (Column("_snuba_col", "events", "column"),)),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f", "avg", (Column("_snuba_col", "events", "_snuba_col"),)
            ),
            {"events": {Column("_snuba_col", None, "column")}},
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f", "avg", (Column("_snuba_col", "events", "_snuba_col"),)
            ),
            {"events": {Column("_snuba_col", None, "column")}},
        ),
        id="Aggregate function over a column. Do not push down",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "countIf",
            (
                Column("_snuba_col", "events", "column"),
                FunctionCall(
                    "_snuba_eq",
                    "equals",
                    (
                        Column("_snuba_col", "events", "column"),
                        Literal(None, "something"),
                    ),
                ),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "countIf",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_eq", "events", "_snuba_eq"),
                ),
            ),
            {
                "events": {
                    Column("_snuba_col", None, "column"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column("_snuba_col", None, "column"),
                            Literal(None, "something"),
                        ),
                    ),
                }
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "countIf",
                (
                    Column("_snuba_col", "events", "_snuba_col"),
                    Column("_snuba_eq", "events", "_snuba_eq"),
                ),
            ),
            {
                "events": {
                    Column("_snuba_col", None, "column"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column("_snuba_col", None, "column"),
                            Literal(None, "something"),
                        ),
                    ),
                }
            },
        ),
        id="Aggregation function that contains a function itself",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_f",
            "f",
            (
                FunctionCall(
                    None,
                    "g",
                    (Column("_snuba_col", "events", "column"), Literal(None, "val")),
                ),
                Column(None, "groups", "column2"),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_gen_1", "events", "_snuba_gen_1"),
                    Column("_snuba_gen_2", "groups", "_snuba_gen_2"),
                ),
            ),
            cut_branches={
                "events": {
                    FunctionCall(
                        "_snuba_gen_1",
                        "g",
                        (Column("_snuba_col", None, "column"), Literal(None, "val"),),
                    )
                },
                "groups": {Column("_snuba_gen_2", None, "column2")},
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "f",
                (
                    Column("_snuba_gen_1", "events", "_snuba_gen_1"),
                    Column("_snuba_gen_2", "groups", "_snuba_gen_2"),
                ),
            ),
            cut_branches={
                "events": {
                    FunctionCall(
                        "_snuba_gen_1",
                        "g",
                        (Column("_snuba_col", None, "column"), Literal(None, "val"),),
                    )
                },
                "groups": {Column("_snuba_gen_2", None, "column2")},
            },
        ),
        id="Nested expressions with alias generation",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_countif",
            "countIf",
            (
                Column("_snuba_events.col", "events", "column"),
                FunctionCall(
                    "_snuba_eq",
                    "equals",
                    (
                        Column("_snuba_groups.col", "groups", "column"),
                        Column("_snuba_events.col", "events", "column"),
                    ),
                ),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_countif",
                "countIf",
                (
                    Column("_snuba_events.col", "events", "_snuba_events.col"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column("_snuba_groups.col", "groups", "_snuba_groups.col"),
                            Column("_snuba_events.col", "events", "_snuba_events.col"),
                        ),
                    ),
                ),
            ),
            {
                "events": {Column("_snuba_events.col", None, "column")},
                "groups": {Column("_snuba_groups.col", None, "column")},
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_countif",
                "countIf",
                (
                    Column("_snuba_events.col", "events", "_snuba_events.col"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column("_snuba_groups.col", "groups", "_snuba_groups.col"),
                            Column("_snuba_events.col", "events", "_snuba_events.col"),
                        ),
                    ),
                ),
            ),
            {
                "events": {Column("_snuba_events.col", None, "column")},
                "groups": {Column("_snuba_groups.col", None, "column")},
            },
        ),
        id="Aggregation function that contains a function that is mixed",
    ),
    pytest.param(
        FunctionCall(
            "_snuba_toUInt64",
            "toUInt64",
            (
                FunctionCall(
                    "_snuba_plus",
                    "plus",
                    (
                        FunctionCall(
                            "_snuba_multiply",
                            "multiply",
                            (
                                FunctionCall(
                                    "_snuba_log",
                                    "log",
                                    (
                                        FunctionCall(
                                            "_snuba_count",
                                            "count",
                                            (
                                                Column(
                                                    "_snuba_events.group_id",
                                                    "events",
                                                    "group_id",
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Literal(None, 600),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_multiply",
                            "multiply",
                            (
                                FunctionCall(
                                    "_snuba_toUInt64",
                                    "toUInt64",
                                    (
                                        FunctionCall(
                                            "_snuba_toUInt64",
                                            "toUInt64",
                                            (
                                                FunctionCall(
                                                    "_snuba_max",
                                                    "max",
                                                    (
                                                        Column(
                                                            "_snuba_events.timestamp",
                                                            "events",
                                                            "timestamp",
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Literal(None, 1000),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_toUInt64",
                "toUInt64",
                (
                    FunctionCall(
                        "_snuba_plus",
                        "plus",
                        (
                            FunctionCall(
                                "_snuba_multiply",
                                "multiply",
                                (
                                    FunctionCall(
                                        "_snuba_log",
                                        "log",
                                        (
                                            FunctionCall(
                                                "_snuba_count",
                                                "count",
                                                (
                                                    Column(
                                                        "_snuba_events.group_id",
                                                        "events",
                                                        "_snuba_events.group_id",
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                    Literal(None, 600),
                                ),
                            ),
                            FunctionCall(
                                "_snuba_multiply",
                                "multiply",
                                (
                                    FunctionCall(
                                        "_snuba_toUInt64",
                                        "toUInt64",
                                        (
                                            FunctionCall(
                                                "_snuba_toUInt64",
                                                "toUInt64",
                                                (
                                                    FunctionCall(
                                                        "_snuba_max",
                                                        "max",
                                                        (
                                                            Column(
                                                                "_snuba_events.timestamp",
                                                                "events",
                                                                "_snuba_events.timestamp",
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                    Literal(None, 1000),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            {
                "events": {
                    Column("_snuba_events.timestamp", None, "timestamp"),
                    Column("_snuba_events.group_id", None, "group_id"),
                }
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_toUInt64",
                "toUInt64",
                (
                    FunctionCall(
                        "_snuba_plus",
                        "plus",
                        (
                            FunctionCall(
                                "_snuba_multiply",
                                "multiply",
                                (
                                    FunctionCall(
                                        "_snuba_log",
                                        "log",
                                        (
                                            FunctionCall(
                                                "_snuba_count",
                                                "count",
                                                (
                                                    Column(
                                                        "_snuba_events.group_id",
                                                        "events",
                                                        "_snuba_events.group_id",
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                    Literal(None, 600),
                                ),
                            ),
                            FunctionCall(
                                "_snuba_multiply",
                                "multiply",
                                (
                                    FunctionCall(
                                        "_snuba_toUInt64",
                                        "toUInt64",
                                        (
                                            FunctionCall(
                                                "_snuba_toUInt64",
                                                "toUInt64",
                                                (
                                                    FunctionCall(
                                                        "_snuba_max",
                                                        "max",
                                                        (
                                                            Column(
                                                                "_snuba_events.timestamp",
                                                                "events",
                                                                "_snuba_events.timestamp",
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                    Literal(None, 1000),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            cut_branches={
                "events": {
                    Column("_snuba_events.group_id", None, "group_id",),
                    Column("_snuba_events.timestamp", None, "timestamp",),
                }
            },
        ),
        id="Nested aggregation function that contains a function that is mixed",
    ),
]


@pytest.mark.parametrize("expression, expected, main_expr", TEST_CASES)
def test_branch_cutter(
    expression: Expression, expected: SubExpression, main_expr: MainQueryExpression
) -> None:
    def alias_generator() -> Generator[str, None, None]:
        i = 0
        while True:
            i += 1
            yield f"_snuba_gen_{i}"

    subexpression = expression.accept(BranchCutter(alias_generator()))
    assert subexpression == expected
    assert subexpression.cut_branch(alias_generator()) == main_expr
