from __future__ import annotations

from typing import Mapping, Optional, Sequence, Type
from unittest.mock import MagicMock

import pytest

import snuba.query.parser.validation.functions as functions
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.parser.validation.functions import FunctionCallsValidator
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall


class FakeValidator(FunctionCallValidator):
    def __init__(self, fails: bool):
        self.__fails = fails

    def validate(self, parameters: Sequence[Expression], schema: ColumnSet) -> None:
        if self.__fails:
            raise InvalidFunctionCall()

        return


test_cases = [
    pytest.param({}, {}, None, id="No validators"),
    pytest.param(
        {"f": FakeValidator(True)},
        {},
        InvalidExpressionException,
        id="Default validator failure",
    ),
    pytest.param(
        {},
        {"f": FakeValidator(True)},
        InvalidExpressionException,
        id="Dataset validator failure",
    ),
    pytest.param(
        {"f": FakeValidator(False), "g": FakeValidator(True)},
        {"k": FakeValidator(True)},
        None,
        id="No failure",
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
        None, "f", (Column(alias=None, table_name=None, column_name="col"),)
    )
    if exception is None:
        FunctionCallsValidator().validate(expression, data_source)
    else:
        with pytest.raises(exception):
            FunctionCallsValidator().validate(expression, data_source)

    # TODO: This should use fixture to do this
    setattr(events_entity, "get_function_call_validators", cached)
    functions.default_validators = fn_cached
