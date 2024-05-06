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

    import os

    # from tests.query.parser.test_formula_mql_query import astlogger

    with open(
        os.path.abspath(""),
        "w",
    ) as f:
        f.write(
            """
from datetime import datetime
import re

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
from snuba.query.mql.parser import parse_mql_query_body
from snuba.query.parser.exceptions import ParsingException

test_cases = [
"""
        )
        # for input, dsl_query in astlogger["parse_mql"]:
        # mql, dataset = input
        # f.write(f"pytest.param({repr(mql)},{repr(dataset)},{dsl_query}),\n")
        f.write(
            """
]

@pytest.mark.parametrize("mql, dataset, expected", test_cases)
def test_autogenerated(mql: str, dataset: Dataset, expected: Query) -> None:
    actual = parse_mql_query_body(mql, dataset)
    assert actual == expected


def test_mismatch_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by status_code"
    with pytest.raises(
        Exception,
        match=re.escape("All terms in a formula must have the same groupby"),
    ):
        parse_mql_query_body(str(query_body), get_dataset("generic_metrics"))


def test_invalid_mri() -> None:
    mql = 'sum(`transaction.duration`){dist:["dist1", "dist2"]}'
    expected = ParsingException("MQL endpoint only supports MRIs")
    with pytest.raises(type(expected), match=re.escape(str(expected))):
        parse_mql_query_body(mql, get_dataset("generic_metrics"))


def test_invalid_mql() -> None:
    mql = "sum(`transaction.duration"
    expected = ParsingException("Parsing error on line 1 at 'um(`transacti'")
    with pytest.raises(type(expected), match=re.escape(str(expected))):
        parse_mql_query_body(mql, get_dataset("generic_metrics"))


@pytest.mark.xfail(reason="Not implemented yet")
def test_apdex1() -> None:
    query_body = "apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 123)"
    parse_mql_query_body(query_body, get_dataset("generic_metrics"))


@pytest.mark.xfail(reason="Not implemented yet")
def test_apdex2() -> None:
    query_body = 'apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}'
    parse_mql_query_body(query_body, get_dataset("generic_metrics"))
"""
        )


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
