import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state.rate_limit import ORGANIZATION_RATE_LIMIT_NAME

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_org_id", None, "org_id"),
            Literal(None, 1),
        ),
        1,
        id="simple org column",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column("_snuba_org_id", None, "org_id"),
            FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 2))),
        ),
        2,
        id="multiple org column",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_org_id", None, "org_id"),
                Literal(None, 3),
            ),
            binary_condition(
                ConditionFunctions.IN,
                Column("_snuba_org_id", None, "org_id"),
                FunctionCall(None, "array", (Literal(None, 4), Literal(None, 5))),
            ),
        ),
        3,
        id="all sorts of orgs",
    ),
]


@pytest.mark.parametrize("unprocessed, org_id", tests)
def test_org_rate_limit_processor(unprocessed: Expression, org_id: int) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPRequestSettings()

    num_before = len(settings.get_rate_limit_params())
    OrganizationRateLimiterProcessor("org_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == ORGANIZATION_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(org_id)
    assert rate_limiter.per_second_limit == 1000
    assert rate_limiter.concurrent_limit == 1000


@pytest.mark.parametrize("unprocessed, org_id", tests)
def test_org_rate_limit_processor_overridden(
    unprocessed: Expression, org_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPRequestSettings()
    state.set_config(f"org_per_second_limit_{org_id}", 5)
    state.set_config(f"org_concurrent_limit_{org_id}", 10)

    num_before = len(settings.get_rate_limit_params())
    OrganizationRateLimiterProcessor("org_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == ORGANIZATION_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(org_id)
    assert rate_limiter.per_second_limit == 5
    assert rate_limiter.concurrent_limit == 10
