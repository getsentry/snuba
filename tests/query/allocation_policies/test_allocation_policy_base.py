from __future__ import annotations

import pytest

from snuba.clusters.storage_sets import StorageSetKey
from snuba.query.allocation_policies import (
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicyViolation,
    PassthroughPolicy,
    QuotaAllowance,
)
from snuba.state import set_config


def test_eq() -> None:
    class SomeAllocationPolicy(PassthroughPolicy):
        pass

    assert PassthroughPolicy(
        StorageSetKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    ) == PassthroughPolicy(
        StorageSetKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    )

    assert PassthroughPolicy(
        StorageSetKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    ) != SomeAllocationPolicy(
        StorageSetKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    )


@pytest.mark.redis_db
def test_passthrough_allows_queries() -> None:
    set_config("query_settings/max_threads", 420)
    assert DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}).can_run
    assert DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}).max_threads == 420


def test_raises_on_false_can_run():
    class RejectingAllocationPolicy(PassthroughPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(can_run=False, max_threads=1, explanation={})

    with pytest.raises(AllocationPolicyViolation):
        RejectingAllocationPolicy(StorageSetKey("something"), []).get_quota_allowance(
            {}
        )
