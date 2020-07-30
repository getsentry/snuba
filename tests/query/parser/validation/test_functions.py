from __future__ import annotations

from typing import Mapping, Optional, Sequence, Type
from unittest.mock import MagicMock

import pytest

import snuba.query.parser.validation.functions as functions
from snuba.clickhouse.columns import ColumnSet
from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.parser.exceptions import (
    FunctionValidationException,
    ValidationException,
)
from snuba.query.parser.validation.functions import FunctionCallsValidator
from snuba.query.validation import FunctionCallValidator
from snuba.state import set_config


class FakeValidator(FunctionCallValidator):
    def __init__(self, fails: bool):
        self.__fails = fails

    def validate(self, parameters: Sequence[Expression], schema: ColumnSet) -> None:
        if self.__fails:
            raise FunctionValidationException()

        return


test_cases = [
    pytest.param({}, {}, None, id="No validators"),
    pytest.param(
        {"f": FakeValidator(True)},
        {},
        FunctionValidationException,
        id="Default validator failure",
    ),
    pytest.param(
        {},
        {"f": FakeValidator(True)},
        FunctionValidationException,
        id="Dataset validator failure",
    ),
    pytest.param(
        {"f": FakeValidator(False), "g": FakeValidator(True)},
        {"k": FakeValidator(True)},
        None,
        id="No failure",
    ),
]


@pytest.mark.parametrize(
    "default_validators, dataset_validators, exception", test_cases
)
def test_functions(
    default_validators: Mapping[str, FunctionCallValidator],
    dataset_validators: Mapping[str, FunctionCallValidator],
    exception: Optional[Type[ValidationException]],
) -> None:
    set_config("enforce_expression_validation", 1)
    functions.default_validators = default_validators
    dataset = MagicMock()
    dataset.get_function_call_validators.return_value = dataset_validators
    dataset.get_abstract_columnset.return_value = ColumnSet([])

    expression = FunctionCall(
        None, "f", (Column(alias=None, table_name=None, column_name="col"),)
    )
    if exception is None:
        FunctionCallsValidator().validate(expression, dataset)
    else:
        with pytest.raises(exception):
            FunctionCallsValidator().validate(expression, dataset)
