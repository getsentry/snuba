from typing import List, Optional, Set

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    in_condition,
)
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.processors.physical.abstract_array_join_optimizer import (
    get_multiple_columns_filters,
    get_single_column_filters,
)
from snuba.query.processors.physical.arrayjoin_optimizer import ArrayJoinOptimizer
from snuba.query.processors.physical.bloom_filter_optimizer import BloomFilterOptimizer
from snuba.query.query_settings import HTTPQuerySettings
from tests.query.processors.query_builders import build_query

spans_ops = Column(None, None, "spans.op")

spans_op_col = FunctionCall("spans_op", "arrayJoin", (spans_ops,))

spans_groups = Column(None, None, "spans.group")

spans_group_col = FunctionCall("spans_group", "arrayJoin", (spans_groups,))

spans_op_group_col = FunctionCall(
    None,
    "tuple",
    (spans_op_col, spans_group_col),
)

spans_exclusive_time_col = FunctionCall(
    "spans_exclusive_time",
    "arrayJoin",
    (Column(None, None, "spans.exclusive_time"),),
)

spans_op_filter_tests = [
    pytest.param(
        build_query(),
        set(),
        id="no op filter",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                ConditionFunctions.EQ,
                spans_op_col,
                Literal(None, "db"),
            ),
        ),
        {"db"},
        id="simple equality",
    ),
    pytest.param(
        build_query(
            condition=in_condition(
                spans_op_col,
                [Literal(None, "db"), Literal(None, "http")],
            ),
        ),
        {"db", "http"},
        id="op IN condition",
    ),
    pytest.param(
        build_query(
            condition=in_condition(
                spans_op_col,
                [Literal(None, "db")],
            ),
            having=in_condition(
                spans_op_col,
                [Literal(None, "http")],
            ),
        ),
        {"db", "http"},
        id="conditions and having",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                BooleanFunctions.OR,
                spans_op_col,
                in_condition(
                    spans_op_col,
                    [Literal(None, "db"), Literal(None, "http")],
                ),
            ),
            having=in_condition(
                spans_op_col,
                [Literal(None, "db"), Literal(None, "http")],
            ),
        ),
        set(),
        id="op OR condition",
    ),
]


@pytest.mark.parametrize("query, expected_result", spans_op_filter_tests)
def test_get_single_column_filters(
    query: ClickhouseQuery, expected_result: Set[Expression]
) -> None:
    """
    Test the algorithm identifies conditions on op/group that can potentially
    be pre-filtered through arrayFilter.
    """
    assert set(get_single_column_filters(query, "spans.op")) == expected_result


spans_op_group_tuple_filter_tests = [
    pytest.param(
        build_query(),
        set(),
        id="no op+group filter",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                ConditionFunctions.EQ,
                spans_op_group_col,
                FunctionCall(None, "tuple", (Literal(None, "db"), Literal(None, "a" * 16))),
            ),
        ),
        {("db", "a" * 16)},
        id="simple equality",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                ConditionFunctions.IN,
                spans_op_group_col,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "a" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "b" * 16)),
                        ),
                    ),
                ),
            ),
        ),
        {("db", "a" * 16), ("http", "b" * 16)},
        id="op+group IN condition",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                ConditionFunctions.IN,
                spans_op_group_col,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "a" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "b" * 16)),
                        ),
                    ),
                ),
            ),
            having=binary_condition(
                ConditionFunctions.IN,
                spans_op_group_col,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "c" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "d" * 16)),
                        ),
                    ),
                ),
            ),
        ),
        {("db", "a" * 16), ("http", "b" * 16), ("db", "c" * 16), ("http", "d" * 16)},
        id="conditions and having",
    ),
    pytest.param(
        build_query(
            condition=binary_condition(
                BooleanFunctions.OR,
                spans_op_group_col,
                binary_condition(
                    ConditionFunctions.IN,
                    spans_op_group_col,
                    FunctionCall(
                        None,
                        "tuple",
                        (
                            FunctionCall(
                                None,
                                "tuple",
                                (Literal(None, "db"), Literal(None, "a" * 16)),
                            ),
                            FunctionCall(
                                None,
                                "tuple",
                                (Literal(None, "http"), Literal(None, "b" * 16)),
                            ),
                        ),
                    ),
                ),
            ),
            having=binary_condition(
                ConditionFunctions.IN,
                spans_op_group_col,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "c" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "d" * 16)),
                        ),
                    ),
                ),
            ),
        ),
        set(),
        id="op+group OR condition",
    ),
]


@pytest.mark.parametrize("query, expected_result", spans_op_group_tuple_filter_tests)
def test_get_multiple_columns_filters(
    query: ClickhouseQuery, expected_result: Set[Expression]
) -> None:
    """
    Test the algorithm identifies conditions on the tuple (op, group) that can potentially
    be pre-filtered through arrayFilter.
    """
    assert set(get_multiple_columns_filters(query, ("spans.op", "spans.group"))) == expected_result


def array_join_col(ops=None, groups=None, op_groups=None):
    conditions: List[Expression] = []

    argument_name = "arg"
    argument = Argument(None, argument_name)

    if ops:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                tupleElement(None, argument, Literal(None, 1)),
                FunctionCall(None, "tuple", tuple(Literal(None, op) for op in ops)),
            )
        )

    if groups:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                tupleElement(None, argument, Literal(None, 2)),
                FunctionCall(None, "tuple", tuple(Literal(None, group) for group in groups)),
            )
        )

    if op_groups:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        tupleElement(None, argument, Literal(None, 1)),
                        tupleElement(None, argument, Literal(None, 2)),
                    ),
                ),
                FunctionCall(
                    None,
                    "tuple",
                    tuple(
                        FunctionCall(None, "tuple", (Literal(None, op), Literal(None, group)))
                        for op, group in op_groups
                    ),
                ),
            )
        )

    cols = FunctionCall(
        None,
        "arrayMap",
        (
            Lambda(
                None,
                ("x", "y", "z"),
                FunctionCall(None, "tuple", tuple(Argument(None, arg) for arg in ("x", "y", "z"))),
            ),
            Column(None, None, "spans.op"),
            Column(None, None, "spans.group"),
            Column(None, None, "spans.exclusive_time"),
        ),
    )

    if conditions:
        cols = FunctionCall(
            None,
            "arrayFilter",
            (
                Lambda(None, (argument_name,), combine_and_conditions(conditions)),
                cols,
            ),
        )

    return arrayJoin("snuba_all_spans", cols)


span_processor_tests = [
    pytest.param(
        build_query(),
        [],
        None,
        id="no spans columns in select clause",
    ),
    pytest.param(
        build_query(selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col]),
        [
            SelectedExpression(
                "spans_op", tupleElement("spans_op", array_join_col(), Literal(None, 1))
            ),
            SelectedExpression(
                "spans_group",
                tupleElement("spans_group", array_join_col(), Literal(None, 2)),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement("spans_exclusive_time", array_join_col(), Literal(None, 3)),
            ),
        ],
        None,
        id="simple array join with all op, group, exclusive_time",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=binary_condition(
                ConditionFunctions.EQ,
                spans_op_col,
                Literal(None, "db"),
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement("spans_op", array_join_col(ops=["db"]), Literal(None, 1)),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement("spans_group", array_join_col(ops=["db"]), Literal(None, 2)),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement("spans_exclusive_time", array_join_col(ops=["db"]), Literal(None, 3)),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
            binary_condition(
                ConditionFunctions.EQ,
                tupleElement("spans_op", array_join_col(ops=["db"]), Literal(None, 1)),
                Literal(None, "db"),
            ),
        ),
        id="simple equals filter on op",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=in_condition(
                spans_op_col,
                [Literal(None, "db"), Literal(None, "http")],
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement("spans_op", array_join_col(ops=["db", "http"]), Literal(None, 1)),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement("spans_group", array_join_col(ops=["db", "http"]), Literal(None, 2)),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement(
                    "spans_exclusive_time",
                    array_join_col(ops=["db", "http"]),
                    Literal(None, 3),
                ),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.OR,
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                FunctionCall(None, "has", (spans_ops, Literal(None, "http"))),
            ),
            in_condition(
                tupleElement("spans_op", array_join_col(ops=["db", "http"]), Literal(None, 1)),
                [Literal(None, "db"), Literal(None, "http")],
            ),
        ),
        id="simple IN filter on op",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=binary_condition(
                ConditionFunctions.EQ,
                spans_group_col,
                Literal(None, "a" * 16),
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement("spans_op", array_join_col(groups=["a" * 16]), Literal(None, 1)),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement("spans_group", array_join_col(groups=["a" * 16]), Literal(None, 2)),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement(
                    "spans_exclusive_time",
                    array_join_col(groups=["a" * 16]),
                    Literal(None, 3),
                ),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
            binary_condition(
                ConditionFunctions.EQ,
                tupleElement("spans_group", array_join_col(groups=["a" * 16]), Literal(None, 2)),
                Literal(None, "a" * 16),
            ),
        ),
        id="simple equals filter on groups",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=in_condition(
                spans_group_col,
                [Literal(None, "a" * 16), Literal(None, "b" * 16)],
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement(
                    "spans_op",
                    array_join_col(groups=["a" * 16, "b" * 16]),
                    Literal(None, 1),
                ),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement(
                    "spans_group",
                    array_join_col(groups=["a" * 16, "b" * 16]),
                    Literal(None, 2),
                ),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement(
                    "spans_exclusive_time",
                    array_join_col(groups=["a" * 16, "b" * 16]),
                    Literal(None, 3),
                ),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.OR,
                FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                FunctionCall(None, "has", (spans_groups, Literal(None, "b" * 16))),
            ),
            in_condition(
                tupleElement(
                    "spans_group",
                    array_join_col(groups=["a" * 16, "b" * 16]),
                    Literal(None, 2),
                ),
                [Literal(None, "a" * 16), Literal(None, "b" * 16)],
            ),
        ),
        id="simple IN filter on groups",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=binary_condition(
                ConditionFunctions.EQ,
                spans_op_group_col,
                FunctionCall(None, "tuple", (Literal(None, "db"), Literal(None, "a" * 16))),
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement(
                    "spans_op",
                    array_join_col(op_groups=[("db", "a" * 16)]),
                    Literal(None, 1),
                ),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement(
                    "spans_group",
                    array_join_col(op_groups=[("db", "a" * 16)]),
                    Literal(None, 2),
                ),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement(
                    "spans_exclusive_time",
                    array_join_col(op_groups=[("db", "a" * 16)]),
                    Literal(None, 3),
                ),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.AND,
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        tupleElement(
                            "spans_op",
                            array_join_col(op_groups=[("db", "a" * 16)]),
                            Literal(None, 1),
                        ),
                        tupleElement(
                            "spans_group",
                            array_join_col(op_groups=[("db", "a" * 16)]),
                            Literal(None, 2),
                        ),
                    ),
                ),
                FunctionCall(None, "tuple", (Literal(None, "db"), Literal(None, "a" * 16))),
            ),
        ),
        id="simple equals filter on op + group",
    ),
    pytest.param(
        build_query(
            selected_columns=[spans_op_col, spans_group_col, spans_exclusive_time_col],
            condition=binary_condition(
                ConditionFunctions.IN,
                spans_op_group_col,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "a" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "b" * 16)),
                        ),
                    ),
                ),
            ),
        ),
        [
            SelectedExpression(
                "spans_op",
                tupleElement(
                    "spans_op",
                    array_join_col(op_groups=[("db", "a" * 16), ("http", "b" * 16)]),
                    Literal(None, 1),
                ),
            ),
            SelectedExpression(
                "spans_group",
                tupleElement(
                    "spans_group",
                    array_join_col(op_groups=[("db", "a" * 16), ("http", "b" * 16)]),
                    Literal(None, 2),
                ),
            ),
            SelectedExpression(
                "spans_exclusive_time",
                tupleElement(
                    "spans_exclusive_time",
                    array_join_col(op_groups=[("db", "a" * 16), ("http", "b" * 16)]),
                    Literal(None, 3),
                ),
            ),
        ],
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    BooleanFunctions.OR,
                    FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                    FunctionCall(None, "has", (spans_ops, Literal(None, "http"))),
                ),
                binary_condition(
                    BooleanFunctions.OR,
                    FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                    FunctionCall(None, "has", (spans_groups, Literal(None, "b" * 16))),
                ),
            ),
            binary_condition(
                ConditionFunctions.IN,
                FunctionCall(
                    None,
                    "tuple",
                    (
                        tupleElement(
                            "spans_op",
                            array_join_col(op_groups=[("db", "a" * 16), ("http", "b" * 16)]),
                            Literal(None, 1),
                        ),
                        tupleElement(
                            "spans_group",
                            array_join_col(op_groups=[("db", "a" * 16), ("http", "b" * 16)]),
                            Literal(None, 2),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    "tuple",
                    (
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "db"), Literal(None, "a" * 16)),
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, "http"), Literal(None, "b" * 16)),
                        ),
                    ),
                ),
            ),
        ),
        id="simple IN filter on op + group",
    ),
]


@pytest.mark.parametrize(
    "query, expected_selected_columns, expected_conditions", span_processor_tests
)
def test_spans_processor(
    query: ClickhouseQuery,
    expected_selected_columns: List[SelectedExpression],
    expected_conditions: Optional[Expression],
) -> None:
    query_settings = HTTPQuerySettings()
    bloom_filter_processor = BloomFilterOptimizer("spans", ["op", "group"], ["exclusive_time"])
    bloom_filter_processor.process_query(query, query_settings)
    array_join_processor = ArrayJoinOptimizer("spans", ["op", "group"], ["exclusive_time"])
    array_join_processor.process_query(query, query_settings)
    assert query.get_selected_columns() == expected_selected_columns
    assert query.get_condition() == expected_conditions
