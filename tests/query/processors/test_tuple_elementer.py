from __future__ import annotations

import pytest

from snuba.query.dsl import identity as dsl_identity
from snuba.query.dsl import literals_tuple, tupleElement
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.processors.tuple_elementer import TupleElementer
from snuba.request.request_settings import HTTPQuerySettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query


def equals(op1: Expression, op2: Expression, alias: str | None = None) -> FunctionCall:
    return FunctionCall(alias, "eq", tuple([op1, op2]))


def some_tuple(*tuple_elems):
    return literals_tuple(None, [Literal(None, e) for e in tuple_elems])


def identity(expression: Expression) -> Expression:
    return dsl_identity(expression, None)


def arrayjoin_pattern():
    """The TupleElementer does not support calculated tuples. One such place
    these occur is the arrayjoin optimizer. They look like this:

    tupleElement(
        arrayJoin(
          arrayMap(
            (x,y ->
              tuple(
                x,
                y
              )
            ),
            tags.key,
            tags.value
          )
        ) AS `snuba_all_tags`,
        2
      ) AS `_snuba_tags_value` |> tags_value
    The TupleElementer should not touch them
    """
    return FunctionCall(
        alias="_snuba_tags_key",
        function_name="tupleElement",
        parameters=(
            FunctionCall(
                alias="snuba_all_tags",
                function_name="arrayJoin",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="arrayMap",
                        parameters=(
                            Lambda(
                                alias=None,
                                parameters=("x", "y"),
                                transformation=FunctionCall(
                                    alias=None,
                                    function_name="tuple",
                                    parameters=(
                                        Argument(alias=None, name="x"),
                                        Argument(alias=None, name="y"),
                                    ),
                                ),
                            ),
                            Column(alias=None, table_name=None, column_name="tags.key"),
                            Column(
                                alias=None, table_name=None, column_name="tags.value"
                            ),
                        ),
                    ),
                ),
            ),
            Literal(alias=None, value=1),
        ),
    )


TEST_QUERIES = [
    pytest.param(
        build_query(
            selected_columns=[
                some_tuple("duration", 300),
                equals(
                    tupleElement(None, some_tuple("duration", 300), Literal(None, 2)),
                    Literal(None, 300),
                ),
            ]
        ),
        build_query(
            selected_columns=[
                some_tuple("duration", 300),
                equals(
                    Literal(None, 300),
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
                                    None, some_tuple("foo", 123), Literal(None, 1)
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
                                Literal(None, "foo"),
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
        # We do not want to optimize the arrayjoin pattern
        # because that is not something we can determine statically
        build_query(selected_columns=[arrayjoin_pattern()]),
        build_query(selected_columns=[arrayjoin_pattern()]),
        id="Arrayjoin pattern tuples not expanded",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                Lambda(
                    None,
                    ("a",),
                    tupleElement(None, some_tuple("foo", "bar"), Literal(None, 1)),
                )
            ]
        ),
        build_query(selected_columns=[Lambda(None, ("a",), Literal(None, "foo"))]),
        id="simple lambda",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                Lambda(
                    None,
                    ("a",),
                    tupleElement(None, arrayjoin_pattern(), Literal(None, 1)),
                )
            ]
        ),
        build_query(
            selected_columns=[
                Lambda(
                    None,
                    ("a",),
                    tupleElement(None, arrayjoin_pattern(), Literal(None, 1)),
                )
            ]
        ),
        id="unoptimized lambda",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "topK",
                        (tupleElement(None, some_tuple(1, 2), Literal(None, 2)),),
                    ),
                    (some_tuple(1, 2, 3, 4),),
                )
            ]
        ),
        build_query(
            selected_columns=[
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "topK",
                        (Literal(None, 2),),
                    ),
                    (some_tuple(1, 2, 3, 4),),
                )
            ]
        ),
        id="curried function",
    ),
]


INVALID_TEST_QUERIES = [
    pytest.param(
        build_query(
            selected_columns=[
                some_tuple("duration", 300),
                equals(
                    tupleElement(
                        None, some_tuple("duration", 300), identity(Literal(None, 2))
                    ),
                    Literal(None, 300),
                ),
            ]
        ),
        id="tupleEment must be accessed with a literal",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                some_tuple("duration", 300),
                equals(
                    tupleElement(
                        None, some_tuple("duration", 300), Literal(None, "not an int")
                    ),
                    Literal(None, 300),
                ),
            ]
        ),
        id="tupleEment must be accessed with an int",
    ),
]


@pytest.mark.parametrize("input_query,expected_query", TEST_QUERIES)
def test_tuple_unaliaser(input_query, expected_query):
    set_config("tuple_elementer_rollout", 1)
    settings = HTTPQuerySettings()
    TupleElementer().process_query(input_query, settings)
    assert input_query == expected_query


@pytest.mark.parametrize("input_query", INVALID_TEST_QUERIES)
def test_invalid_tuples(input_query):
    set_config("tuple_elementer_rollout", 1)
    settings = HTTPQuerySettings()
    with pytest.raises(InvalidQueryException):
        TupleElementer().process_query(input_query, settings)


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
