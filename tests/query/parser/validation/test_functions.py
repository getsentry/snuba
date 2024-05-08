from __future__ import annotations

from typing import Mapping, Optional, Sequence, Type
from unittest.mock import MagicMock

import pytest

import snuba.query.parser.validation.functions as functions
from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source import DataSource
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl_mapper import query_repr
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.parser.validation.functions import FunctionCallsValidator
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall
from snuba.query.validation.functions import AllowedFunctionValidator


class FakeValidator(FunctionCallValidator):
    def __init__(self, fails: bool):
        self.__fails = fails

    def validate(
        self, func_name: str, parameters: Sequence[Expression], data_source: DataSource
    ) -> None:
        if self.__fails:
            raise InvalidFunctionCall()

        return


test_cases = [
    pytest.param({}, {}, None, id="No validators"),
    pytest.param(
        {"and": FakeValidator(True)},
        {},
        InvalidExpressionException,
        id="Default validator failure",
    ),
    pytest.param(
        {},
        {"and": FakeValidator(True)},
        InvalidExpressionException,
        id="Dataset validator failure",
    ),
    pytest.param(
        {"and": FakeValidator(False), "or": FakeValidator(True)},
        {"in": FakeValidator(True)},
        None,
        id="No failure",
    ),
]

test_expressions = [
    pytest.param(
        FunctionCall(
            None,
            "f",
            (Column(alias=None, table_name=None, column_name="col"),),
        ),
        True,
        id="Invalid function name",
    ),
    pytest.param(
        FunctionCall(
            None,
            "count",
            (Column(alias=None, table_name=None, column_name="col"),),
        ),
        False,
        id="Valid function name",
    ),
]


@pytest.mark.parametrize("default_validators, entity_validators, exception", test_cases)
def test_functions(
    default_validators: Mapping[str, FunctionCallValidator],
    entity_validators: Mapping[str, FunctionCallValidator],
    exception: Optional[Type[InvalidExpressionException]],
) -> None:
    fn_cached = functions.default_validators
    functions.default_validators = default_validators

    entity_return = MagicMock()
    entity_return.return_value = entity_validators
    events_entity = get_entity(EntityKey.EVENTS)
    cached = events_entity.get_function_call_validators
    setattr(events_entity, "get_function_call_validators", entity_return)
    data_source = QueryEntity(EntityKey.EVENTS, ColumnSet([]))

    expression = FunctionCall(
        None, "and", (Column(alias=None, table_name=None, column_name="col"),)
    )
    if exception is None:
        FunctionCallsValidator().validate(expression, data_source)
    else:
        with pytest.raises(exception):
            FunctionCallsValidator().validate(expression, data_source)

    # TODO: This should use fixture to do this
    setattr(events_entity, "get_function_call_validators", cached)
    functions.default_validators = fn_cached


@pytest.mark.parametrize("expression, should_raise", test_expressions[:1])
@pytest.mark.redis_db
def test_invalid_function_name(expression: FunctionCall, should_raise: bool) -> None:
    data_source = QueryEntity(EntityKey.EVENTS, ColumnSet([]))
    state.set_config("function-validator.enabled", True)

    with pytest.raises(InvalidExpressionException):
        FunctionCallsValidator().validate(expression, data_source)

    kylewrite()


@pytest.mark.parametrize("expression, should_raise", test_expressions)
@pytest.mark.redis_db
def test_allowed_functions_validator(
    expression: FunctionCall, should_raise: bool
) -> None:
    data_source = QueryEntity(EntityKey.EVENTS, ColumnSet([]))
    state.set_config("function-validator.enabled", True)

    if should_raise:
        with pytest.raises(InvalidFunctionCall):
            AllowedFunctionValidator().validate(
                expression.function_name, expression.parameters, data_source
            )
    else:
        AllowedFunctionValidator().validate(
            expression.function_name, expression.parameters, data_source
        )


def kylewrite() -> None:
    import os

    from tests.query.parser.test_formula_mql_query import astlogger

    funcs = [
        ("parse_mql_query_body", ("tuple[str, Dataset]", "Query")),
        (
            "populate_query_from_mql_context",
            ("tuple[Query, dict[str,Any]]", "tuple[Query, MQLContext]"),
        ),
        ("treeify_or_and_conditions", ("Query", "Query")),
        ("post_process", ("Query", "Query")),
        ("post_process_mql", ("Query", "Query")),
        ("replace_time_condition", ("Query", "Query")),
    ]
    paths = [f"tests/query/parser/unit_tests/test_{f[0]}2.py" for f in funcs]
    for i in range(1, len(funcs)):  # len(fnames)
        with open(
            os.path.abspath(paths[i]),
            "w",
        ) as f:
            f.write(
                """
import re
from datetime import datetime
from typing import Any

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    column,
    divide,
    equals,
    greaterOrEquals,
    in_cond,
    less,
    literal,
    literals_tuple,
    multiply,
    plus,
)
from snuba.query.expressions import CurriedFunctionCall
from snuba.query.logical import Query
from snuba.query.mql.mql_context import MetricsScope, MQLContext, Rollup
from snuba.query.mql.parser import parse_mql_query_body, populate_query_from_mql_context
from snuba.query.parser.exceptions import ParsingException
"""
            )
            f.write("test_cases = [")
            failures = []
            for trace in astlogger:
                if i >= len(trace):
                    continue
                elif (i == len(trace) - 1) and trace[i][1] is None:
                    failures.append(trace)
                else:
                    f.write(f"pytest.param({trace[i][0]}, {trace[i][1]}),\n")
            f.write("]\n\n")
            f.write('\n@pytest.mark.parametrize("theinput, expected", test_cases)\n')
            f.write(
                f"def test_autogenerated(theinput: {funcs[i][1][0]}, expected: {funcs[i][1][1]}) -> None:\n"
            )
            f.write(f"    actual = {funcs[i][0]}({'*theinput'})\n")
            f.write("    assert actual == expected")
            f.write("\n\n")
            f.write("failure_cases = [\n")
            for e in failures:
                f.write(f"{e[0]},\n")
            f.write("]")
