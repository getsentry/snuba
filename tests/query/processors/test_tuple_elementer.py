from __future__ import annotations

import pytest

from snuba.query.dsl import identity as dsl_identity
from snuba.query.dsl import literals_tuple, tupleElement
from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.processors.tuple_elementer import TupleElementer
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query


def equals(op1: Expression, op2: Expression, alias: str | None = None) -> FunctionCall:
    return FunctionCall(alias, "eq", tuple([op1, op2]))


def some_tuple(alias: str | None):
    return literals_tuple(alias, [Literal(None, "duration"), Literal(None, 300)])


def identity(expression: Expression) -> Expression:
    return dsl_identity(expression, None)


TEST_QUERIES = [
    pytest.param(
        build_query(
            selected_columns=[
                some_tuple(alias="foo"),
                equals(
                    tupleElement(None, some_tuple(alias="doo"), Literal(None, 1)),
                    Literal(None, 300),
                ),
            ]
        ),
        build_query(
            selected_columns=[
                some_tuple(alias="foo"),
                equals(
                    Literal(None, "duration"),
                    Literal(None, 300),
                ),
            ]
        ),
        id="simple happy path",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                identity(
                    identity(
                        identity(
                            equals(
                                tupleElement(
                                    None, some_tuple(alias="ayyy"), Literal(None, 1)
                                ),
                                Literal(None, 300),
                            )
                        )
                    )
                )
            ]
        ),
        build_query(
            selected_columns=[
                identity(
                    identity(
                        identity(
                            equals(
                                Literal(None, "duration"),
                                Literal(None, 300),
                            )
                        )
                    )
                )
            ]
        ),
        id="Highly nested",
    )
]


@pytest.mark.parametrize("input_query,expected_query", TEST_QUERIES)
def test_tuple_unaliaser(input_query, expected_query):
    set_config("tuple_elementer_rollout", 1)
    settings = HTTPRequestSettings()
    TupleElementer().process_query(input_query, settings)
    assert input_query == expected_query


def test_killswitch():
    p = TupleElementer()
    assert not p.should_run()
    set_config("tuple_elementer_rollout", 0)
    assert not p.should_run()
    set_config("tuple_elementer_rollout", 1)
    assert p.should_run()


def test_garbage_rollot():
    p = TupleElementer()
    set_config("tuple_elementer_rollout", "garbage")
    assert not p.should_run()
