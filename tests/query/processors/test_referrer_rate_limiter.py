import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.object_id_rate_limiter import (
    ReferrerRateLimiterProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.rate_limit import REFERRER_RATE_LIMIT_NAME

conditions = [
    binary_condition(
        ConditionFunctions.EQ,
        Column("_snuba_project_id", None, "project_id"),
        Literal(None, 1),
    ),
    binary_condition(
        ConditionFunctions.IN,
        Column("_snuba_project_id", None, "project_id"),
        FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 2))),
    ),
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
]

queries = [
    Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=c,
    )
    for c in conditions
]


def test_project_rate_limit_processor() -> None:

    settings = HTTPQuerySettings(referrer="foo")

    num_before = len(settings.get_rate_limit_params())
    for query in queries:
        ReferrerRateLimiterProcessor().process_query(query, settings)
    # if the limiter is not configured, do not apply it
    assert len(settings.get_rate_limit_params()) == num_before


@pytest.mark.redis_db
def test_project_rate_limit_processor_overridden() -> None:
    referrer_name = "foo"
    settings = HTTPQuerySettings(referrer="foo")
    state.set_config(f"referrer_per_second_limit_{referrer_name}", 5)
    state.set_config(f"referrer_concurrent_limit_{referrer_name}", 10)

    num_before = len(settings.get_rate_limit_params())
    for query in queries:
        ReferrerRateLimiterProcessor().process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == REFERRER_RATE_LIMIT_NAME
    assert rate_limiter.bucket == referrer_name
    assert rate_limiter.per_second_limit == 5
    assert rate_limiter.concurrent_limit == 10


@pytest.mark.redis_db
def test_project_rate_limit_processor_overridden_only_one() -> None:
    referrer_name = "foo"
    settings = HTTPQuerySettings(referrer="foo")
    state.set_config(f"referrer_concurrent_limit_{referrer_name}", 10)

    num_before = len(settings.get_rate_limit_params())
    for query in queries:
        ReferrerRateLimiterProcessor().process_query(query, settings)
    assert len(settings.get_rate_limit_params()) == num_before + 1
    rate_limiter = settings.get_rate_limit_params()[-1]
    assert rate_limiter.rate_limit_name == REFERRER_RATE_LIMIT_NAME
    assert rate_limiter.bucket == referrer_name
    assert rate_limiter.per_second_limit is None
    assert rate_limiter.concurrent_limit == 10
