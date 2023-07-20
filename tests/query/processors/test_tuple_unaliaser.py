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
from snuba.query.processors.physical.tuple_unaliaser import TupleUnaliaser
from snuba.query.query_settings import HTTPQuerySettings
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
                # top level tuple alias persists
                some_tuple(alias="foo"),
                equals(
                    # alias of the tuple of internal function is removed (it is not useful)
                    tupleElement(None, some_tuple(alias=None), Literal(None, 1)),
                    Literal(None, 300),
                ),
            ]
        ),
        id="simple happy path",
    ),
    pytest.param(
        build_query(selected_columns=[Lambda(None, ("a",), some_tuple(alias="foo"))]),
        build_query(selected_columns=[Lambda(None, ("a",), some_tuple(alias=None))]),
        id="simple lambda",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                identity(
                    identity(
                        identity(
                            equals(
                                # alias of the tuple of internal function is removed (it is not useful)
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
                                # alias of the tuple of internal function is removed (it is not useful)
                                tupleElement(
                                    None, some_tuple(alias=None), Literal(None, 1)
                                ),
                                Literal(None, 300),
                            )
                        )
                    )
                )
            ]
        ),
        id="Highly nested",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                CurriedFunctionCall(
                    None, some_tuple(alias="foo"), (Literal(None, "4"),)
                )
            ]
        ),
        build_query(
            selected_columns=[
                CurriedFunctionCall(None, some_tuple(alias=None), (Literal(None, "4"),))
            ]
        ),
        id="curried function",
    ),
]


@pytest.mark.parametrize("input_query,expected_query", TEST_QUERIES)
@pytest.mark.redis_db
def test_tuple_unaliaser(input_query, expected_query):
    set_config("tuple_unaliaser_rollout", 1)
    settings = HTTPQuerySettings()
    TupleUnaliaser().process_query(input_query, settings)
    assert input_query == expected_query
