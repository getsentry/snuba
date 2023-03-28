from typing import Optional

import pytest

from snuba import state
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.quota_processor import (
    ENABLED_CONFIG,
    REFERRER_CONFIG,
    REFERRER_ORGANIZATION_CONFIG,
    REFERRER_PROJECT_CONFIG,
    ResourceQuotaProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.quota import ResourceQuota

referrer_tests = [
    # Referrer
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_CONFIG}_some_referrer",
        None,
        ResourceQuota(max_threads=5),
        id="all referrers",
    ),
    # Referrer + Project
    pytest.param(
        0,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        None,
        None,
        id="Processor disabled",
    ),
    pytest.param(
        1,
        "some_other_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        None,
        None,
        id="Different referrer",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_2",
        None,
        None,
        id="Different project",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        None,
        ResourceQuota(max_threads=5),
        id="Apply quota",
    ),
    # Organization + Referrer
    pytest.param(
        0,
        "some_referrer",
        f"{REFERRER_ORGANIZATION_CONFIG}_some_referrer_10",
        10,
        None,
        id="Processor disabled (org)",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_ORGANIZATION_CONFIG}_some_referrer_10",
        10,
        ResourceQuota(max_threads=5),
        id="Apply quota for org",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_ORGANIZATION_CONFIG}_some_referrer_10",
        11,
        None,
        id="Different org",
    ),
    pytest.param(
        1,
        "some_other_referrer",
        f"{REFERRER_ORGANIZATION_CONFIG}_some_referrer_10",
        10,
        None,
        id="Different referrer for org",
    ),
]


@pytest.mark.parametrize(
    "enabled, referrer, config_to_set, organization_id, expected_quota", referrer_tests
)
@pytest.mark.redis_db
def test_apply_quota(
    enabled: int,
    referrer: str,
    config_to_set: str,
    organization_id: Optional[int],
    expected_quota: Optional[ResourceQuota],
) -> None:
    state.set_config(ENABLED_CONFIG, enabled)
    state.set_config(config_to_set, 5)

    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
    )
    settings = HTTPQuerySettings(organization_id=organization_id)
    settings.referrer = referrer

    ResourceQuotaProcessor("project_id").process_query(query, settings)
    assert settings.get_resource_quota() == expected_quota


@pytest.mark.redis_db
def test_apply_overlapping_quota() -> None:
    referrer = "MYREFERRER"
    referrer_project_limited_project_id = 1337
    referrer_limited_project_id = 314
    referrer_quota = 20
    referrer_project_quota = 5

    state.set_config(ENABLED_CONFIG, 1)
    state.set_config(f"referrer_thread_quota_{referrer}", referrer_quota)
    state.set_config(
        f"referrer_project_thread_quota_{referrer}_{referrer_project_limited_project_id}",
        referrer_project_quota,
    )

    # test the limit with the referrer_project config
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, referrer_project_limited_project_id),
        ),
    )
    settings = HTTPQuerySettings()
    settings.referrer = referrer

    ResourceQuotaProcessor("project_id").process_query(query, settings)
    # see that the more restrictive quota is applied
    assert settings.get_resource_quota() == ResourceQuota(
        max_threads=referrer_project_quota
    )

    # test with just the referrer limit applied
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=binary_condition(
            ConditionFunctions.EQ,
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, referrer_limited_project_id),
        ),
    )
    settings = HTTPQuerySettings()
    settings.referrer = referrer

    ResourceQuotaProcessor("project_id").process_query(query, settings)
    # see that just the referrer limit was applied given that there was no config for that
    # specific project id
    assert settings.get_resource_quota() == ResourceQuota(max_threads=referrer_quota)
