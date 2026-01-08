from typing import Optional

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.conditions import combine_and_conditions
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.array_has_optimizer import ArrayHasOptimizer
from snuba.query.query_settings import HTTPQuerySettings
from tests.query.processors.query_builders import build_query

spans_ops = Column(None, None, "spans.op")
spans_groups = Column(None, None, "spans.group")

array_has_tests = [
    pytest.param(build_query(), None, id="no conditions"),
    pytest.param(
        build_query(
            condition=FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
        ),
        FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
        id="one has condition",
    ),
    pytest.param(
        build_query(
            condition=combine_and_conditions(
                [
                    FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                    FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                ],
            ),
        ),
        combine_and_conditions(
            [
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
            ],
        ),
        id="two has conditions",
    ),
    pytest.param(
        build_query(
            condition=FunctionCall(
                None,
                "equals",
                (
                    FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                    Literal(None, 1),
                ),
            ),
        ),
        FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
        id="one has condition wrapped in equals 1",
    ),
    pytest.param(
        build_query(
            condition=FunctionCall(
                None,
                "equals",
                (
                    FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                    Literal(None, 0),
                ),
            ),
        ),
        FunctionCall(
            None,
            "equals",
            (
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                Literal(None, 0),
            ),
        ),
        id="one has condition wrapped in equals 0",
    ),
    pytest.param(
        build_query(
            condition=combine_and_conditions(
                [
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                            Literal(None, 1),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                            Literal(None, 1),
                        ),
                    ),
                ],
            ),
        ),
        combine_and_conditions(
            [
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
            ],
        ),
        id="two has conditions wrapped in equals 1",
    ),
    pytest.param(
        build_query(
            condition=combine_and_conditions(
                [
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                            Literal(None, 0),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                            Literal(None, 0),
                        ),
                    ),
                ],
            ),
        ),
        combine_and_conditions(
            [
                FunctionCall(
                    None,
                    "equals",
                    (
                        FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                        Literal(None, 0),
                    ),
                ),
                FunctionCall(
                    None,
                    "equals",
                    (
                        FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                        Literal(None, 0),
                    ),
                ),
            ],
        ),
        id="two has conditions wrapped in equals 0",
    ),
    pytest.param(
        build_query(
            condition=combine_and_conditions(
                [
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                            Literal(None, 1),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "equals",
                        (
                            FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                            Literal(None, 0),
                        ),
                    ),
                ],
            ),
        ),
        combine_and_conditions(
            [
                FunctionCall(None, "has", (spans_ops, Literal(None, "db"))),
                FunctionCall(
                    None,
                    "equals",
                    (
                        FunctionCall(None, "has", (spans_groups, Literal(None, "a" * 16))),
                        Literal(None, 0),
                    ),
                ),
            ],
        ),
        id="two has conditions one wrapped in equals 1, other wrapped in equals 0",
    ),
]


@pytest.mark.parametrize("query, expected_conditions", array_has_tests)
def test_array_has_optimizer(
    query: ClickhouseQuery,
    expected_conditions: Optional[Expression],
) -> None:
    query_settings = HTTPQuerySettings()
    array_has_processor = ArrayHasOptimizer(["spans.op", "spans.group"])
    array_has_processor.process_query(query, query_settings)
    assert query.get_condition() == expected_conditions
