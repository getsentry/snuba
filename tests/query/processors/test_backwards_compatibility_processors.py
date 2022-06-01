"""This file tests the query processors introduced to facilitate backwards
compatibility with between Clickhouse 21.8 and CLickhouse 20.7"""
from __future__ import annotations

from copy import deepcopy

import pytest

from snuba.query.dsl import literals_tuple, tupleElement
from snuba.query.expressions import FunctionCall, Literal
from snuba.query.processors.tuple_elementer import TupleElementer
from snuba.query.processors.tuple_unaliaser import TupleUnaliaser
from snuba.request.request_settings import HTTPQuerySettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query

TEST_QUERIES = [
    pytest.param(
        build_query(
            selected_columns=[
                literals_tuple("foo", (Literal(None, "a"), Literal(None, "b"))),
                FunctionCall(
                    None,
                    "equals",
                    (
                        tupleElement(
                            None,
                            literals_tuple(
                                "doo", (Literal(None, "c"), Literal(None, "d"))
                            ),
                            Literal(None, 1),
                        ),
                        Literal(None, 300),
                    ),
                ),
            ]
        ),
        build_query(
            selected_columns=[
                literals_tuple("foo", (Literal(None, "a"), Literal(None, "b"))),
                FunctionCall(
                    None,
                    "equals",
                    (
                        Literal(None, "c"),
                        Literal(None, 300),
                    ),
                ),
            ]
        ),
        id="a tuple element and an unalias",
    )
]


@pytest.mark.parametrize("input_query,expected_query", TEST_QUERIES)
def test_processors(input_query, expected_query):
    set_config("tuple_elementer_rollout", 1)
    set_config("tuple_unaliaser_rollout", 1)

    def test_processors(i_query, processors):
        q_copy = deepcopy(i_query)
        for p in processors:
            p.process_query(q_copy, HTTPQuerySettings())
        assert q_copy == expected_query

    q_processors = [TupleElementer(), TupleUnaliaser()]
    # order should not matter, the resulting query should be the same
    test_processors(input_query, q_processors)
    test_processors(input_query, reversed(q_processors))
