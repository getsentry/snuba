from typing import Optional

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.dataset import QuotaExceeded
from snuba.datasets.entities import EntityKey
from snuba.datasets.quota.project_quota_control import (
    QUOTA_ENFORCEMENT_ENABLED,
    ProjectQuotaControl,
)
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state import get_config, set_config
from snuba.state.rate_limit import RateLimitStats

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_not_project", None, "not_project_id"),
            Literal(None, 1),
        ),
        1,
        5,
        True,
        None,
        id="No project id condition",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        0,
        5,
        True,
        None,
        id="Different project id",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 3))),
        ),
        1,
        5,
        True,
        RateLimitStats(0.0, 1),
        id="Valid project id",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 2),
            ),
            binary_condition(
                ConditionFunctions.IN,
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "array", (Literal(None, 6), Literal(None, 7))),
            ),
        ),
        1,
        5,
        True,
        RateLimitStats(0.0, 1),
        id="Valid project id complex condition",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 3))),
        ),
        1,
        0,
        False,
        None,
        id="Quota exceeded",
    ),
]


@pytest.mark.parametrize("condition, enabled, quota, success, expected", tests)
def test_project_rate_limit_processor(
    condition: Expression,
    enabled: bool,
    quota: int,
    success: bool,
    expected: Optional[RateLimitStats],
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=condition,
    )
    settings = HTTPRequestSettings()
    request = Request("asdasd", {}, query, settings)

    config = get_config(QUOTA_ENFORCEMENT_ENABLED)
    set_config(QUOTA_ENFORCEMENT_ENABLED, enabled)
    set_config("project_per_second_limit_2", 10.0)
    set_config("project_concurrent_limit_2", quota)

    if success:
        with ProjectQuotaControl("project_id").acquire(request) as stats:
            assert stats == expected
    else:
        with pytest.raises(QuotaExceeded):
            with ProjectQuotaControl("project_id").acquire(request) as stats:
                pass

    set_config(QUOTA_ENFORCEMENT_ENABLED, config)
