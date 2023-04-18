from __future__ import annotations

from unittest import mock

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicyViolation,
    InvalidPolicyConfig,
    PassthroughPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.state import set_config


def test_eq() -> None:
    class SomeAllocationPolicy(PassthroughPolicy):
        pass

    assert PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    ) == PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    )

    assert PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    ) != SomeAllocationPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
    )


@pytest.mark.redis_db
def test_passthrough_allows_queries() -> None:
    set_config("query_settings/max_threads", 420)
    assert DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}).can_run
    assert DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}).max_threads == 420


def test_raises_on_false_can_run():
    class RejectingEverythingAllocationPolicy(PassthroughPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(can_run=False, max_threads=1, explanation={})

    with pytest.raises(AllocationPolicyViolation):
        RejectingEverythingAllocationPolicy(
            StorageKey("something"), []
        ).get_quota_allowance({})


def test_passes_through_on_error() -> None:
    class BadlyWrittenAllocationPolicy(PassthroughPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            raise AttributeError("You messed up!")

        def _update_quota_balance(
            self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
        ) -> None:
            raise ValueError("you messed up AGAIN")

    with pytest.raises(AttributeError):
        BadlyWrittenAllocationPolicy(StorageKey("something"), []).get_quota_allowance(
            {}
        )

    with pytest.raises(ValueError):
        BadlyWrittenAllocationPolicy(StorageKey("something"), []).update_quota_balance(None, None)  # type: ignore

    # should not raise even though the implementation is buggy (this is the production setting)
    with mock.patch("snuba.settings.RAISE_ON_ALLOCATION_POLICY_FAILURES", False):
        assert (
            BadlyWrittenAllocationPolicy(StorageKey("something"), [])
            .get_quota_allowance({})
            .can_run
        )

        BadlyWrittenAllocationPolicy(StorageKey("something"), []).update_quota_balance(
            None, None  # type: ignore
        )


@pytest.mark.redis_db
def test_bad_config_keys() -> None:
    policy = PassthroughPolicy(StorageKey("something"), [])
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config("bad_config", 1)
    assert str(err.value) == "'bad_config' is not a valid config for PassthroughPolicy!"
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config("is_active", "bad_value")
    assert str(err.value) == "'bad_value' (str) is not of expected type: int"
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config("is_enforced", "bad_value")
    assert str(err.value) == "'bad_value' (str) is not of expected type: int"

    with pytest.raises(InvalidPolicyConfig) as err:
        policy.get_config("does_not_exist")
    assert (
        str(err.value)
        == "'does_not_exist' is not a valid config for PassthroughPolicy!"
    )
