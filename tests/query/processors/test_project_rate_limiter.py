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
    ProjectRateLimiterProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.rate_limit import PROJECT_RATE_LIMIT_NAME

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        1,
        id="simple project column",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 2))),
        ),
        2,
        id="multiple project column",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "array", (Literal(None, 2), Literal(None, 2))),
        ),
        2,
        id="array project column",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 3),
            ),
            binary_condition(
                ConditionFunctions.IN,
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "array", (Literal(None, 3), Literal(None, 5))),
            ),
        ),
        3,
        id="all sorts of projects",
    ),
]


@pytest.mark.parametrize("unprocessed, project_id", tests)
@pytest.mark.redis_db
def test_project_rate_limit_processor(unprocessed: Expression, project_id: int) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPQuerySettings()

    num_before = len(settings.get_rate_limit_params())
    ProjectRateLimiterProcessor("project_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == PROJECT_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(project_id)
    assert rate_limiter.per_second_limit == 1000
    assert rate_limiter.concurrent_limit == 1000


@pytest.mark.parametrize("unprocessed, project_id", tests)
@pytest.mark.redis_db
def test_project_rate_limit_processor_overridden(
    unprocessed: Expression, project_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPQuerySettings()
    state.set_config(f"project_per_second_limit_{project_id}", 5)
    state.set_config(f"project_concurrent_limit_{project_id}", 10)

    num_before = len(settings.get_rate_limit_params())
    ProjectRateLimiterProcessor("project_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == PROJECT_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(project_id)
    assert rate_limiter.per_second_limit == 5
    assert rate_limiter.concurrent_limit == 10
