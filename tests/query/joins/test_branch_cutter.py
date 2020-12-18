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
            Column(None, "events", "_snuba_col"),
            cut_branches={"events": {Column("_snuba_col", None, "column")}},
        ),
        id="Basic Column to push down",
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
            Column(None, "events", "_snuba_tag"),
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
            Column(None, "events", "_snuba_f"),
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
            Column(None, "events", "_snuba_f"),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "groups", "_snuba_col2"),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "groups", "_snuba_col2"),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "groups", "_snuba_col2"),
                    Column(None, "groups", "_snuba_col3"),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "groups", "_snuba_col2"),
                    Column(None, "groups", "_snuba_col3"),
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
                    Column(None, "events", "_snuba_g"),
                    Column(None, "groups", "_snuba_col2"),
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
                    Column(None, "events", "_snuba_g"),
                    Column(None, "groups", "_snuba_col2"),
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
            Column(None, "events", "_snuba_cf"),
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
            FunctionCall("_snuba_f", "avg", (Column(None, "events", "_snuba_col"),)),
            {"events": {Column("_snuba_col", None, "column")}},
        ),
        MainQueryExpression(
            FunctionCall("_snuba_f", "avg", (Column(None, "events", "_snuba_col"),)),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "events", "_snuba_eq"),
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
                    Column(None, "events", "_snuba_col"),
                    Column(None, "events", "_snuba_eq"),
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
            "countIf",
            (
                Column("_snuba_col", "events", "column"),
                FunctionCall(
                    "_snuba_eq",
                    "equals",
                    (
                        Column("_snuba_col", "groups", "column"),
                        Column("_snuba_col", "events", "column"),
                    ),
                ),
            ),
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "countIf",
                (
                    Column(None, "events", "_snuba_col"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column(None, "groups", "_snuba_col"),
                            Column(None, "events", "_snuba_col"),
                        ),
                    ),
                ),
            ),
            {
                "events": {Column("_snuba_col", None, "column")},
                "groups": {Column("_snuba_col", None, "column")},
            },
        ),
        MainQueryExpression(
            FunctionCall(
                "_snuba_f",
                "countIf",
                (
                    Column(None, "events", "_snuba_col"),
                    FunctionCall(
                        "_snuba_eq",
                        "equals",
                        (
                            Column(None, "groups", "_snuba_col"),
                            Column(None, "events", "_snuba_col"),
                        ),
                    ),
                ),
            ),
            {
                "events": {Column("_snuba_col", None, "column")},
                "groups": {Column("_snuba_col", None, "column")},
            },
        ),
        id="Aggregation function that contains a function that is mixed",
    ),
]


@pytest.mark.parametrize("expression, expected, main_expr", TEST_CASES)
def test_branch_cutter(
    expression: Expression, expected: SubExpression, main_expr: MainQueryExpression
) -> None:
    subexpression = expression.accept(BranchCutter())
    assert subexpression == expected
    assert subexpression.cut_branch() == main_expr
