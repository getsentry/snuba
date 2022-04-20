from __future__ import annotations

import pytest

from snuba.query.dsl import literals_tuple, tupleElement
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.processors.tuple_unaliaser import TupleUnaliaser
from snuba.request.request_settings import HTTPRequestSettings
from tests.query.processors.query_builders import build_query


def equals(op1: Expression, op2: Expression, alias: str | None = None) -> FunctionCall:
    return FunctionCall(alias, "eq", tuple([op1, op2]))


def some_tuple(alias: str | None):
    return literals_tuple(alias, [Literal(None, "duration"), Literal(None, 300)])


bad_alias = tupleElement(None, some_tuple(alias="doo"), Literal(None, 1))

TEST_QUERIES = [
    pytest.param(
        build_query(
            selected_columns=[
                some_tuple(alias="foo"),
                equals(bad_alias, Literal(None, 300)),
            ]
        ),
        build_query(
            selected_columns=[
                some_tuple(alias="foo"),
                equals(
                    tupleElement(None, some_tuple(alias=None), Literal(None, 1)),
                    Literal(None, 300),
                ),
            ]
        ),
        id="simple happy path",
    )
]


@pytest.mark.parametrize("input_query,expected_query", TEST_QUERIES)
def test_tuple_unaliaser(input_query, expected_query):
    settings = HTTPRequestSettings()
    TupleUnaliaser().process_query(input_query, settings)
    assert input_query == expected_query
    assert repr(input_query) == repr(expected_query)
