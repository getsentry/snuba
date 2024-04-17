from dataclasses import replace

import pytest

from snuba import settings as snubasettings
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
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
from snuba.query.data_source.simple import Entity, Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.schemas import UUID, String, UInt


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

mockmetadata = SnubaQueryMetadata(
    Request(
        "",
        {},
        LogicalQuery(
            from_clause=Entity(key=EntityKey.TRANSACTIONS, schema=EntityColumnSet([]))
        ),
        HTTPQuerySettings(),
        AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
        "",
    ),
    "blah",
    Timer("woof"),
)

ch_query = Query(
    from_clause=Table(
        "transactions_local",
        ColumnSet(
            [
                ("event_id", UUID()),
                ("project_id", UInt(64)),
                ("transaction_name", String()),
                ("duration", UInt(32)),
            ]
        ),
        storage_key=StorageKey.TRANSACTIONS,
        allocation_policies=[policy],
    ),
    selected_columns=[
        SelectedExpression(
            "avg(duration)",
            FunctionCall(
                "_snuba_avg(duration)",
                "avg",
                (Column("_snuba_duration", None, "duration"),),
            ),
        )
    ],
    condition=and_cond(
        equals(column("transaction_name"), literal("dog")),
        equals(column("project_id"), literal(1)),
    ),
    limit=1000,
)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic() -> None:
    attinfo = AttributionInfo(
        get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
    )
    settings = HTTPQuerySettings()
    timer = Timer("test")
    res = ExecutionStage(attinfo, query_metadata=mockmetadata).execute(
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
        and "avg(duration)" in res.data.result["data"][0]
    )
    assert policy.did_update
    q = settings.get_resource_quota()
    assert q and q.max_threads == 1


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_dry_run() -> None:
    attinfo = AttributionInfo(
        get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
    )
    # set dry run
    settings = HTTPQuerySettings(dry_run=True)
    timer = Timer("test")
    res = ExecutionStage(attinfo, query_metadata=mockmetadata).execute(
        QueryPipelineResult(
            data=ch_query,
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )
    assert (
        res.data
        and res.data.result["data"] == []
        and res.data.result["meta"] == []
        and "cluster_name" in res.data.extra["stats"]
        and res.data.extra["sql"]
        and "experiments" in res.data.extra
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_turbo() -> None:
    ch_query.set_from_clause(
        replace(
            ch_query.get_from_clause(),
            table_name="transactions_local",
            storage_key=StorageKey.TRANSACTIONS,
        )
    )
    attinfo = AttributionInfo(
        get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
    )
    settings = HTTPQuerySettings(turbo=True)
    timer = Timer("test")
    res = ExecutionStage(attinfo, query_metadata=mockmetadata).execute(
        QueryPipelineResult(
            data=ch_query,
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )
    assert res.data
    assert (
        res.data
        and len(res.data.result["data"]) == 1
        and "avg(duration)" in res.data.result["data"][0]
    )
    assert ch_query.get_from_clause().sampling_rate == snubasettings.TURBO_SAMPLE_RATE
