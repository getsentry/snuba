import pytest

from snuba import state
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
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
            ConditionFunctions.IN,
            Column("_snuba_org_id", None, "org_id"),
            FunctionCall(None, "array", (Literal(None, 2), Literal(None, 2))),
        ),
        2,
        id="array org column",
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
                FunctionCall(None, "array", (Literal(None, 3), Literal(None, 5))),
            ),
        ),
        3,
        id="all sorts of orgs",
    ),
]


@pytest.mark.parametrize("unprocessed, org_id", tests)
def test_org_rate_limit_processor(unprocessed: Expression, org_id: int) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPQuerySettings()

    num_before = len(settings.get_rate_limit_params())
    OrganizationRateLimiterProcessor("org_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == ORGANIZATION_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(org_id)
    assert rate_limiter.per_second_limit == 1000
    assert rate_limiter.concurrent_limit == 1000


@pytest.mark.parametrize("unprocessed, org_id", tests)
@pytest.mark.redis_db
def test_org_rate_limit_processor_overridden(
    unprocessed: Expression, org_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPQuerySettings()
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


@pytest.mark.redis_db
def test_namespaced_rate_limit() -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_org_id", None, "org_id"),
            Literal(None, 1),
        ),
    )
    org_id = 1
    settings = HTTPQuerySettings()
    ps_key = f"org_per_second_limit_{org_id}"
    ct_key = f"org_concurrent_limit_{org_id}"
    state.set_config(ps_key, 5)
    state.set_config(ct_key, 10)

    OrganizationRateLimiterProcessor("org_id").process_query(query, settings)

    # Check that the rate limit parameters were copied to the new namespace
    ps_found = state.get_uncached_config(ps_key, config_key=state.rate_limit_config_key)
    ct_found = state.get_uncached_config(ct_key, config_key=state.rate_limit_config_key)

    assert ps_found == 5
    assert ct_found == 10

    # Check that the correct values were found and used
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == ORGANIZATION_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(org_id)
    assert rate_limiter.per_second_limit == 5
    assert rate_limiter.concurrent_limit == 10
