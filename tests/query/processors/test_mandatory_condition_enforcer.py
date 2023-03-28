from datetime import datetime

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.dsl import literals_array
from snuba.query.expressions import Column, Literal
from snuba.query.processors.condition_checkers.checkers import (
    OrgIdEnforcer,
    ProjectIdEnforcer,
)
from snuba.query.processors.physical.conditions_enforcer import (
    MandatoryConditionEnforcer,
)
from snuba.query.query_settings import HTTPQuerySettings
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
        OrgIdEnforcer(),
        id="Valid query. Both mandatory columns are there",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.IN,
                    Column(None, None, "project_id"),
                    literals_array(None, [Literal(None, 123)]),
                ),
                binary_condition(
                    "equals", Column(None, None, "org_id"), Literal(None, 1)
                ),
            ),
        ),
        True,
        OrgIdEnforcer(),
        id="Valid query. Both mandatory columns are there (in array)",
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
        OrgIdEnforcer(),
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
        OrgIdEnforcer(),
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
        OrgIdEnforcer(),
        id="Invalid query. Or condition",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                in_condition(Column(None, None, "project_id"), [Literal(None, 123)]),
                binary_condition(
                    "equals", Column(None, None, "organization_id"), Literal(None, 1)
                ),
            ),
        ),
        True,
        OrgIdEnforcer("organization_id"),
        id="Valid query. Both mandatory columns are there",
    ),
    pytest.param(
        Query(
            Table("errors", ColumnSet([])),
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                in_condition(Column(None, None, "project_id"), [Literal(None, 123)]),
                binary_condition(
                    "equals",
                    Column(None, None, "bad_organization_id"),
                    Literal(None, 1),
                ),
            ),
        ),
        False,
        OrgIdEnforcer("organization_id"),
        id="Invalid query. Only one mandatory column",
    ),
]


@pytest.mark.parametrize("query, valid, org_id_enforcer", test_data)
@pytest.mark.redis_db
def test_condition_enforcer(
    query: Query, valid: bool, org_id_enforcer: OrgIdEnforcer
) -> None:
    set_config("mandatory_condition_enforce", 1)
    query_settings = HTTPQuerySettings(consistent=True)
    processor = MandatoryConditionEnforcer([org_id_enforcer, ProjectIdEnforcer()])
    if valid:
        processor.process_query(query, query_settings)
    else:
        with pytest.raises(AssertionError):
            processor.process_query(query, query_settings)
