from datetime import datetime

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_execution import ExecutionStage
from snuba.query import SelectedExpression
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, binary_condition, column, equals, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.schemas import DateTime, UInt


class MockAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        # Define policy specific config definitions, these will be used along
        # with the default definitions of the base class. (is_enforced, is_active)
        return []

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(can_run=True, max_threads=1, explanation={})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        self.did_update = True


policy = MockAllocationPolicy(
    StorageKey("mystorage"),
    required_tenant_types=["organization_id", "referrer"],
    default_config_overrides={},
)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic():
    attinfo = AttributionInfo(
        get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
    )
    settings = HTTPQuerySettings()
    timer = Timer("test")
    policy = MockAllocationPolicy(
        StorageKey.METRICS_DISTRIBUTIONS,
        required_tenant_types=["organization_id", "referrer"],
        default_config_overrides={},
    )
    ch_query = Query(
        from_clause=Table(
            "metrics_distributions_v2_local",
            ColumnSet(
                [
                    ("org_id", UInt(64)),
                    ("project_id", UInt(64)),
                    ("timestamp", DateTime()),
                    ("granularity", UInt(32)),
                ]
            ),
            storage_key=StorageKey.METRICS_DISTRIBUTIONS,
            allocation_policies=[policy],
        ),
        selected_columns=[
            SelectedExpression(
                "avg(granularity)",
                FunctionCall(
                    "_snuba_avg(granularity)",
                    "avg",
                    (Column("_snuba_granularity", None, "granularity"),),
                ),
            )
        ],
        condition=and_cond(
            equals(column("granularity"), literal(60)),
            and_cond(
                binary_condition(
                    "greaterOrEquals",
                    column("timestamp", alias="_snuba_timestamp"),
                    literal(datetime(2021, 5, 17, 19, 42, 1)),
                ),
                and_cond(
                    binary_condition(
                        "less",
                        column("timestamp", alias="_snuba_timestamp"),
                        literal(datetime(2021, 5, 17, 23, 42, 1)),
                    ),
                    and_cond(
                        equals(column("org_id", alias="_snuba_org_id"), literal(1)),
                        equals(
                            column("project_id", alias="_snuba_project_id"), literal(1)
                        ),
                    ),
                ),
            ),
        ),
        limit=1000,
    )
    res = ExecutionStage(
        attinfo,
        query_metadata=SnubaQueryMetadata(
            Request("", {}, ch_query, settings, attinfo, ""),
            "blah",
            timer,
        ),
    ).execute(
        QueryPipelineResult(
            data=ch_query,
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )
    assert (
        res.data
        and len(res.data.result["data"]) == 1
        and "avg(granularity)" in res.data.result["data"][0]
    )
    assert policy.did_update
    assert settings.get_resource_quota().max_threads == 1


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_dry_run():
    attinfo = AttributionInfo(
        get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
    )
    # set dry run
    settings = HTTPQuerySettings(dry_run=True)
    timer = Timer("test")
    ch_query = Query(
        from_clause=Table(
            "metrics_distributions_v2_local",
            ColumnSet(
                [
                    ("org_id", UInt(64)),
                    ("project_id", UInt(64)),
                    ("timestamp", DateTime()),
                    ("granularity", UInt(32)),
                ]
            ),
            storage_key=StorageKey.METRICS_DISTRIBUTIONS,
        ),
        selected_columns=[
            SelectedExpression(
                "avg(granularity)",
                FunctionCall(
                    "_snuba_avg(granularity)",
                    "avg",
                    (Column("_snuba_granularity", None, "granularity"),),
                ),
            )
        ],
        condition=and_cond(
            equals(column("granularity"), literal(60)),
            and_cond(
                binary_condition(
                    "greaterOrEquals",
                    column("timestamp", alias="_snuba_timestamp"),
                    literal(datetime(2021, 5, 17, 19, 42, 1)),
                ),
                and_cond(
                    binary_condition(
                        "less",
                        column("timestamp", alias="_snuba_timestamp"),
                        literal(datetime(2021, 5, 17, 23, 42, 1)),
                    ),
                    and_cond(
                        equals(column("org_id", alias="_snuba_org_id"), literal(1)),
                        equals(
                            column("project_id", alias="_snuba_project_id"), literal(1)
                        ),
                    ),
                ),
            ),
        ),
        limit=1000,
    )
    res = ExecutionStage(
        attinfo,
        query_metadata=SnubaQueryMetadata(
            Request("", {}, ch_query, settings, attinfo, ""),
            "blah",
            timer,
        ),
    ).execute(
        QueryPipelineResult(
            data=ch_query,
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )
    assert (
        res.data.result["data"] == []
        and res.data.result["meta"] == []
        and "cluster_name" in res.data.extra["stats"]
        and res.data.extra["sql"]
        and "experiments" in res.data.extra
    )
