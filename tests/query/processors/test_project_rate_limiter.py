import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.request.request_settings import HTTPRequestSettings
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
            "and",
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 3),
            ),
            binary_condition(
                ConditionFunctions.IN,
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "array", (Literal(None, 4), Literal(None, 5))),
            ),
        ),
        3,
        id="all sorts of projects",
    ),
]


@pytest.mark.parametrize("unprocessed, project_id", tests)
def test_project_rate_limit_processor(unprocessed: Expression, project_id: int) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPRequestSettings()

    num_before = len(settings.get_rate_limit_params())
    ProjectRateLimiterProcessor("project_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == PROJECT_RATE_LIMIT_NAME
    assert rate_limiter.bucket == str(project_id)
    assert rate_limiter.per_second_limit == 1000
    assert rate_limiter.concurrent_limit == 1000


@pytest.mark.parametrize("unprocessed, project_id", tests)
def test_project_rate_limit_processor_overridden(
    unprocessed: Expression, project_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    settings = HTTPRequestSettings()
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
