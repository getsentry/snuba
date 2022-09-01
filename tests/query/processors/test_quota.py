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
from snuba.query.processors.quota_processor import (
    ENABLED_CONFIG,
    REFERRER_PROJECT_CONFIG,
    ResourceQuotaProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.quota import ResourceQuota

tests = [
    pytest.param(
        0,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        None,
        id="Processor disabled",
    ),
    pytest.param(
        1,
        "some_other_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        None,
        id="Different referrer",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_2",
        None,
        id="Different project",
    ),
    pytest.param(
        1,
        "some_referrer",
        f"{REFERRER_PROJECT_CONFIG}_some_referrer_1",
        ResourceQuota(max_threads=5),
        id="Apply quota",
    ),
]


@pytest.mark.parametrize("enabled, referrer, config_to_set, expected_quota", tests)
def test_apply_quota(
    enabled: int,
    referrer: str,
    config_to_set: str,
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
    settings = HTTPQuerySettings()
    settings.referrer = referrer

    ResourceQuotaProcessor("project_id").process_query(query, settings)
    assert settings.get_resource_quota() == expected_quota
