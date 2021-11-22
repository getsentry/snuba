import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.processors.object_id_rate_limiter import ProjectReferrerRateLimiter
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state.rate_limit import PROJECT_REFERRER_RATE_LIMIT_NAME

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        1,
        id="simple proj column",
    ),
]


@pytest.mark.parametrize("unprocessed, project_id", tests)
def test_referrer_rate_limit_processor(
    unprocessed: Expression, project_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    state.set_config("project_referrer_per_second_limit_1_abusive_delivery", 1000)
    state.set_config("project_referrer_concurrent_limit_1_abusive_delivery", 1000)
    referrer = "abusive_delivery"
    settings = HTTPRequestSettings()
    settings.referrer = referrer

    num_before = len(settings.get_rate_limit_params())
    ProjectReferrerRateLimiter("project_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == PROJECT_REFERRER_RATE_LIMIT_NAME
    assert rate_limiter.bucket == f"{project_id}_{referrer}"
    assert rate_limiter.per_second_limit == 1000
    assert rate_limiter.concurrent_limit == 1000


@pytest.mark.parametrize("unprocessed, project_id", tests)
def test_referrer_rate_limit_processor_no_config(
    unprocessed: Expression, project_id: int
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    # don't configure it, rate limit shouldn't fire
    referrer = "abusive_delivery"
    settings = HTTPRequestSettings()
    settings.referrer = referrer

    num_before = len(settings.get_rate_limit_params())
    ProjectReferrerRateLimiter("project_id").process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before
