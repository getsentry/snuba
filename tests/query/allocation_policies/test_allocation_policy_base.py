from __future__ import annotations

from unittest import mock

import pytest

from snuba.clusters.load_info import LoadInfo
from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    InvalidTenantsForAllocationPolicy,
    PassthroughPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.bytes_scanned_policy import (
    BytesScannedPerOrgPolicy,
    BytesScannedPerOrgRefPolicy,
    BytesScannedPerRefPolicy,
)
from snuba.query.allocation_policies.concurrent_queries_policy import (
    ConcurrentQueriesPerOrgPolicy,
    ConcurrentQueriesPerOrgRefPolicy,
    ConcurrentQueriesPerRefPolicy,
)
from snuba.query.allocation_policies.resolver import get_active_allocation_policies
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from tests.query.allocation_policies.attachment import match_block, override_allocation_policy


def test_passthrough_allows_queries() -> None:
    policy = PassthroughPolicy(StorageKey("something"), max_threads=420)
    assert policy.get_quota_allowance({}, "deadbeef").can_run
    assert policy.get_quota_allowance({}, "deadbeef").max_threads == 420


class RejectingEverythingAllocationPolicy(PassthroughPolicy):
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=False,
            max_threads=0,
            explanation={},
            rejection_threshold=MAX_THRESHOLD,
            quota_used=0,
            quota_unit=NO_UNITS,
            suggestion=NO_SUGGESTION,
        )


class BadlyWrittenAllocationPolicy(PassthroughPolicy):
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        raise AttributeError("You messed up!")

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        raise ValueError("you messed up AGAIN")


class InvalidTenantAllocationPolicy(PassthroughPolicy):
    def _get_quota_allowance(self, tenant_ids: dict[str, str | int], query_id: str):
        raise InvalidTenantsForAllocationPolicy.from_args(tenant_ids, self.__class__.__name__)

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ):
        raise InvalidTenantsForAllocationPolicy.from_args(tenant_ids, self.__class__.__name__)


def test_passes_through_on_error() -> None:
    with pytest.raises(AttributeError):
        BadlyWrittenAllocationPolicy(StorageKey("something")).get_quota_allowance(
            {}, query_id="deadbeef"
        )

    with pytest.raises(ValueError):
        BadlyWrittenAllocationPolicy(StorageKey("something")).update_quota_balance(
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
        )

    # should not raise even though the implementation is buggy (this is the production setting)
    with mock.patch("snuba.settings.RAISE_ON_ALLOCATION_POLICY_FAILURES", False):
        assert (
            BadlyWrittenAllocationPolicy(StorageKey("something"))
            .get_quota_allowance({}, query_id="deadbeef")
            .can_run
        )

        BadlyWrittenAllocationPolicy(StorageKey("something")).update_quota_balance(
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
        )

        assert (
            not InvalidTenantAllocationPolicy(StorageKey("Something"))
            .get_quota_allowance({"some": "tenant"}, "12345")
            .can_run
        )

        InvalidTenantAllocationPolicy(StorageKey("Something")).update_quota_balance(
            {"some": "tenant"},
            "12345",
            None,  # type: ignore[arg-type]
        )


@pytest.mark.redis_db
def test_is_not_enforced() -> None:
    MAX_THREADS = 100
    reject_policy = RejectingEverythingAllocationPolicy(
        StorageKey("some_storage"), max_threads=MAX_THREADS
    )
    not_enforced_policy = RejectingEverythingAllocationPolicy(
        StorageKey("some_storage"), enforce="never", max_threads=MAX_THREADS
    )
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    quota_allowance = reject_policy.get_quota_allowance(tenant_ids, "deadbeef")
    assert not quota_allowance.can_run and quota_allowance.max_threads == 0

    # policy not enforced so we don't reject the query
    quota_allowance = not_enforced_policy.get_quota_allowance(tenant_ids, "deadbeef")
    assert quota_allowance.can_run and quota_allowance.max_threads == MAX_THREADS

    rejected_metrics = get_recorded_metric_calls(
        "increment", "allocation_policy.db_request_rejected"
    )
    assert len(rejected_metrics) == 2
    assert rejected_metrics[0].tags["policy_class"] == "RejectingEverythingAllocationPolicy"
    assert rejected_metrics[0].tags["is_enforced"] == "True"
    assert rejected_metrics[0].tags["org_id"] == "123"
    assert rejected_metrics[0].tags["project_id"] == "no_project_id"
    assert rejected_metrics[1].tags["is_enforced"] == "False"


@pytest.mark.redis_db
def test_is_pardonable_overrides_can_run_when_idle() -> None:
    reject_policy = RejectingEverythingAllocationPolicy(
        StorageKey("some_storage"),
        enforce="pardon",
        thresholds={"reject": {"cluster_load": 0.0, "concurrent_queries": 0.0}},
    )
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    idle = LoadInfo(cluster_load=0.0, concurrent_queries=0)
    # No load info => rejected; idle load within thresholds => pardoned. Pardon is
    # observable via the idle_pardon explanation + db_request_pardoned metric, not
    # a flipped can_run (the caller honours the decision).
    assert reject_policy.get_quota_allowance(tenant_ids, "deadbeef").can_run is False
    pardoned = reject_policy.get_quota_allowance(tenant_ids, "deadbeef", idle)
    assert pardoned.can_run is False
    assert pardoned.max_threads == 0
    assert pardoned.explanation["idle_pardon"] == idle.to_dict()

    pardoned_metrics = get_recorded_metric_calls(
        "increment", "allocation_policy.db_request_pardoned"
    )
    assert pardoned_metrics is not None
    assert len(pardoned_metrics) == 1
    assert pardoned_metrics[0].tags["org_id"] == "123"
    rejected_metrics = get_recorded_metric_calls(
        "increment", "allocation_policy.db_request_rejected"
    )
    assert rejected_metrics is not None
    assert len(rejected_metrics) == 1


@pytest.mark.redis_db
def test_idle_pardons_without_reject_thresholds() -> None:
    reject_policy = RejectingEverythingAllocationPolicy(
        StorageKey("some_storage"), enforce="pardon"
    )
    idle = LoadInfo(cluster_load=0.0, concurrent_queries=0)
    allowance = reject_policy.get_quota_allowance(
        {"organization_id": 123, "referrer": "some_referrer"},
        "deadbeef",
        idle,
    )
    assert allowance.explanation["idle_pardon"] == idle.to_dict()


class TestComponentNameBackwardsCompatibility:
    """Test that component_name() keeps the historical ``{storage}.{ClassName}`` shape."""

    def get_all_allocation_policy_classes(self) -> list[type[AllocationPolicy]]:
        return [
            BytesScannedPerOrgPolicy,
            BytesScannedPerOrgRefPolicy,
            BytesScannedPerRefPolicy,
            ConcurrentQueriesPerOrgPolicy,
            ConcurrentQueriesPerOrgRefPolicy,
            ConcurrentQueriesPerRefPolicy,
        ]

    @pytest.mark.parametrize(
        "storage_key",
        [
            StorageKey.DISCOVER,
            StorageKey.EAP_ITEMS,
            StorageKey.EAP_ITEMS_DOWNSAMPLE_8,
            StorageKey.EAP_ITEMS_DOWNSAMPLE_64,
            StorageKey.EAP_ITEMS_DOWNSAMPLE_512,
            StorageKey.ERRORS,
            StorageKey.ERRORS_RO,
            StorageKey.FUNCTIONS,
            StorageKey.FUNCTIONS_RAW,
            StorageKey.METRICS_COUNTERS,
        ],
    )
    def test_component_name_equals_old_runtime_config_prefix_pattern(
        self, storage_key: StorageKey
    ) -> None:
        """component_name() is ``{storage_key}.{ClassName}``."""
        policy_classes = self.get_all_allocation_policy_classes()

        for policy_class in policy_classes:
            # Create an instance of the policy
            policy = policy_class(
                storage_key=storage_key,
            )

            expected_old_prefix = f"{storage_key.value}.{policy_class.__name__}"

            # They should be the same
            assert policy.component_name() == expected_old_prefix


_V2_POLICY_CLASSES: list[type[AllocationPolicy]] = [
    BytesScannedPerOrgPolicy,
    BytesScannedPerOrgRefPolicy,
    BytesScannedPerRefPolicy,
    ConcurrentQueriesPerOrgPolicy,
    ConcurrentQueriesPerOrgRefPolicy,
    ConcurrentQueriesPerRefPolicy,
]


# The v2 Redis keys are built from component_name(), and config names are looked up
# by config_key(). Changing either would reset every live window or break lookups.
@pytest.mark.parametrize("policy_class", [PassthroughPolicy, *_V2_POLICY_CLASSES])
def test_policy_names_are_stable(policy_class: type[AllocationPolicy]) -> None:
    policy = policy_class(ResourceIdentifier(StorageKey.EAP_ITEMS))
    name = policy_class.__name__
    assert policy.component_name() == f"eap_items.{name}"
    assert policy_class.config_key() == f"AllocationPolicy.{name}"
    assert AllocationPolicy.get_from_name(name) is policy_class


@pytest.mark.parametrize("policy_class", _V2_POLICY_CLASSES)
@pytest.mark.parametrize(
    "enforce, is_enforced, is_pardonable",
    [("never", False, False), ("pardon", True, True), ("always", True, False)],
)
def test_enforce_mapping(
    policy_class: type[AllocationPolicy], enforce: str, is_enforced: bool, is_pardonable: bool
) -> None:
    policy = policy_class.from_kwargs(storage_key="eap_items", enforce=enforce)
    assert policy.is_enforced is is_enforced
    assert policy.is_pardonable is is_pardonable


def test_passthrough_default_max_threads() -> None:
    assert PassthroughPolicy(ResourceIdentifier(StorageKey.EAP_ITEMS)).max_threads == 10


def test_resolver_builds_v2_policy_from_named_spec() -> None:
    spec = {"name": "ConcurrentQueriesPerOrgPolicy", "limit": 3, "enforce": "pardon"}
    with override_allocation_policy({"eap_items": match_block([spec])}):
        (policy,) = get_active_allocation_policies(ResourceIdentifier(StorageKey.EAP_ITEMS))
    assert isinstance(policy, ConcurrentQueriesPerOrgPolicy)
    assert policy._limit == 3
    assert policy.is_pardonable
