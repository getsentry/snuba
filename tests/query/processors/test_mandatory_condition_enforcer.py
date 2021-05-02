from datetime import datetime

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.conditions import BooleanFunctions, binary_condition, in_condition
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Literal
from snuba.query.processors.conditions_enforcer import (
    MandatoryConditionEnforcer,
    enforce_org_id,
    enforce_project_id,
)
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state import set_config

test_data = [
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                in_condition(Column(None, None, "project_id"), [Literal(None, 123)]),
                binary_condition(
                    "equals", Column(None, None, "org_id"), Literal(None, 1)
                ),
            ),
        ),
        True,
        id="Valid query. Both mandatory columns are there",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    "equals", Column(None, None, "project_id"), Literal(None, 1)
                ),
                binary_condition(
                    "greaterOrEquals",
                    Column(None, None, "timestamp"),
                    Literal(None, datetime(2020, 1, 1, 12, 0)),
                ),
            ),
        ),
        False,
        id="Invalid query. Only one mandatory column",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    "equals", Column(None, None, "a_col"), Literal(None, 1)
                ),
                binary_condition(
                    "equals", Column(None, None, "another_col"), Literal(None, 1)
                ),
            ),
        ),
        False,
        id="Invalid query. Both mandatory conditions are missing",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.OR,
                binary_condition(
                    "equals", Column(None, None, "project_id"), Literal(None, 1)
                ),
                binary_condition(
                    "equals", Column(None, None, "org_id"), Literal(None, 1)
                ),
            ),
        ),
        False,
        id="Invalid query. Or condition",
    ),
]


@pytest.mark.parametrize("query, valid", test_data)
def test_condition_enforcer(query: Query, valid: bool) -> None:
    set_config("mandatory_condition_enforce", 1)
    request_settings = HTTPRequestSettings(consistent=True)
    processor = MandatoryConditionEnforcer(
        {"project_id": enforce_project_id, "org_id": enforce_org_id}
    )
    if valid:
        processor.process_query(query, request_settings)
    else:
        with pytest.raises(AssertionError):
            processor.process_query(query, request_settings)
