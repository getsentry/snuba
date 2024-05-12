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

    kylewritesnql1sthalf()


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


def kylewritesnql2ndhalf() -> None:
    import os

    from tests.query.parser.test_formula_mql_query import astlogger

    with open(
        os.path.abspath(
            "tests/query/parser/unit_tests/test_post_process_and_validate_query.py"
        ),
        "w",
    ) as f:
        f.write(
            """
import re
from datetime import datetime
from typing import Any, Type

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    NestedColumn,
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
    or_cond,
    plus,
)
from snuba.query.expressions import Argument, CurriedFunctionCall, Lambda
from snuba.query.logical import Query
from snuba.query.mql.mql_context import MetricsScope, MQLContext, Rollup
from snuba.query.mql.parser import (
    MQL_POST_PROCESSORS,
    MQLParseFirstHalf,
    parse_mql_query_body,
    populate_query_from_mql_context,
)
from snuba.query.parser.exceptions import (
    AliasShadowingException,
    CyclicAliasException,
    ParsingException,
)
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import CustomProcessors, parse_snql_query_initial, PostProcessAndValidateQuery
from snuba.utils.metrics.timer import Timer

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")

"""
        )
        f.write("test_cases = [")
        failures = []
        for trace in astlogger:
            curr = trace[0]
            cin, cout = curr
            if isinstance(cout, Exception):
                failures.append((cin, type(cout).__name__))
            else:
                f.write(f"pytest.param({cin}, {cout}),\n")
        f.write("]\n\n")
        f.write(
            """
@pytest.mark.parametrize("theinput, expected", test_cases)
def test_autogenerated(theinput: tuple[
            Query | CompositeQuery[LogicalDataSource],
            Dataset,
            QuerySettings | None,
            CustomProcessors | None,
        ], expected: Query | CompositeQuery[LogicalDataSource]) -> None:
    # dummy timer and settings dont matter
    dummy_timer = Timer("snql_pipeline")
    dummy_settings = HTTPQuerySettings()
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=theinput,
            error=None,
            query_settings=dummy_settings,
            timer=dummy_timer,
        )
    )
    assert res.data and not res.error
    eq, reason = res.data.equals(expected)
    assert eq, reason
"""
        )
        f.write("\n\n")
        f.write("failure_cases = [\n")
        for e in failures:
            cin, err_type = e
            f.write(f"pytest.param({cin}, {err_type}),\n")
        f.write("]\n\n")
        f.write(
            """
@pytest.mark.parametrize("theinput, expected_error", failure_cases)
def test_autogenerated_invalid(theinput: tuple[
            Query | CompositeQuery[LogicalDataSource],
            Dataset,
            QuerySettings | None,
            CustomProcessors | None,
        ], expected_error: Type[Exception]) -> None:
    # dummy timer and settings dont matter
    dummy_timer = Timer("snql_pipeline")
    dummy_settings = HTTPQuerySettings()
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=theinput,
            error=None,
            query_settings=dummy_settings,
            timer=dummy_timer,
        )
    )
    assert res.error and not res.data
    assert isinstance(res.error, expected_error)
"""
        )


def kylewritesnql1sthalf() -> None:
    import os

    from tests.query.parser.test_formula_mql_query import astlogger

    with open(
        os.path.abspath(
            "tests/query/parser/unit_tests/test_post_process_and_validate_query.py"
        ),
        "w",
    ) as f:
        f.write(
            """
import re
from datetime import datetime
from typing import Any, Type

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    NestedColumn,
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
    or_cond,
    plus,
)
from snuba.query.expressions import Argument, CurriedFunctionCall, Lambda
from snuba.query.logical import Query
from snuba.query.mql.mql_context import MetricsScope, MQLContext, Rollup
from snuba.query.mql.parser import (
    MQL_POST_PROCESSORS,
    MQLParseFirstHalf,
    parse_mql_query_body,
    populate_query_from_mql_context,
)
from snuba.query.parser.exceptions import (
    AliasShadowingException,
    CyclicAliasException,
    ParsingException,
)
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import CustomProcessors, parse_snql_query_initial, PostProcessAndValidateQuery
from snuba.utils.metrics.timer import Timer

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")

"""
        )
        f.write("test_cases = [")
        failures = []
        for trace in astlogger:
            curr = trace[0]
            cin, cout = curr
            if isinstance(cout, Exception):
                failures.append((cin, type(cout).__name__))
            else:
                f.write(f"pytest.param({cin}, {cout}),\n")
        f.write("]\n\n")
        f.write(
            """
@pytest.mark.parametrize("theinput, expected", test_cases)
def test_autogenerated(theinput: tuple[
            Query | CompositeQuery[LogicalDataSource],
            Dataset,
            QuerySettings | None,
            CustomProcessors | None,
        ], expected: Query | CompositeQuery[LogicalDataSource]) -> None:
    # dummy timer and settings dont matter
    dummy_timer = Timer("snql_pipeline")
    dummy_settings = HTTPQuerySettings()
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=theinput,
            error=None,
            query_settings=dummy_settings,
            timer=dummy_timer,
        )
    )
    assert res.data and not res.error
    eq, reason = res.data.equals(expected)
    assert eq, reason
"""
        )
        f.write("\n\n")
        f.write("failure_cases = [\n")
        for e in failures:
            cin, err_type = e
            f.write(f"pytest.param({cin}, {err_type}),\n")
        f.write("]\n\n")
        f.write(
            """
@pytest.mark.parametrize("theinput, expected_error", failure_cases)
def test_autogenerated_invalid(theinput: tuple[
            Query | CompositeQuery[LogicalDataSource],
            Dataset,
            QuerySettings | None,
            CustomProcessors | None,
        ], expected_error: Type[Exception]) -> None:
    # dummy timer and settings dont matter
    dummy_timer = Timer("snql_pipeline")
    dummy_settings = HTTPQuerySettings()
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=theinput,
            error=None,
            query_settings=dummy_settings,
            timer=dummy_timer,
        )
    )
    assert res.error and not res.data
    assert isinstance(res.error, expected_error)
"""
        )
