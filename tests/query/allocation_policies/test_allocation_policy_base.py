from __future__ import annotations

from unittest import TestCase, mock

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    CAPMAN_HASH,
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicy,
    AllocationPolicyConfig,
    InvalidPolicyConfig,
    InvalidTenantsForAllocationPolicy,
    PassthroughPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.state import set_config
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from snuba.web import QueryResult


def test_eq() -> None:
    class SomeAllocationPolicy(PassthroughPolicy):
        pass

    assert PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
        default_config_overrides={},
    ) == PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
        default_config_overrides={},
    )

    assert PassthroughPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
        default_config_overrides={},
    ) != SomeAllocationPolicy(
        StorageKey("something"),
        required_tenant_types=["organization_id", "referrer"],
        default_config_overrides={},
    )


@pytest.mark.redis_db
def test_passthrough_allows_queries() -> None:
    DEFAULT_PASSTHROUGH_POLICY.set_config_value("max_threads", 420)
    assert DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}, "deadbeef").can_run
    assert (
        DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance({}, "deadbeef").max_threads
        == 420
    )


class RejectingEverythingAllocationPolicy(PassthroughPolicy):
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(can_run=False, max_threads=10, explanation={})


class ThrottleEverythingAllocationPolicy(PassthroughPolicy):
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(can_run=True, max_threads=1, explanation={})


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
        raise InvalidTenantsForAllocationPolicy.from_args(
            tenant_ids, self.__class__.__name__
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ):
        raise InvalidTenantsForAllocationPolicy.from_args(
            tenant_ids, self.__class__.__name__
        )


def test_passes_through_on_error() -> None:
    with pytest.raises(AttributeError):
        BadlyWrittenAllocationPolicy(
            StorageKey("something"), [], {}
        ).get_quota_allowance({}, query_id="deadbeef")

    with pytest.raises(ValueError):
        BadlyWrittenAllocationPolicy(StorageKey("something"), [], {}).update_quota_balance(None, None, None)  # type: ignore

    # should not raise even though the implementation is buggy (this is the production setting)
    with mock.patch("snuba.settings.RAISE_ON_ALLOCATION_POLICY_FAILURES", False):
        assert (
            BadlyWrittenAllocationPolicy(StorageKey("something"), [], {})
            .get_quota_allowance({}, query_id="deadbeef")
            .can_run
        )

        BadlyWrittenAllocationPolicy(
            StorageKey("something"), [], {}
        ).update_quota_balance(
            None, None, None  # type: ignore
        )

        assert (
            not InvalidTenantAllocationPolicy(StorageKey("Something"), [], {})
            .get_quota_allowance({"some": "tenant"}, "12345")
            .can_run
        )

        InvalidTenantAllocationPolicy(StorageKey("Something"), [], {}).update_quota_balance({"some": "tenant"}, "12345", None)  # type: ignore


@pytest.mark.redis_db
def test_bad_config_keys() -> None:
    policy = PassthroughPolicy(StorageKey("something"), [], {})
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value("bad_config", 1)
    assert str(err.value) == "'bad_config' is not a valid config for PassthroughPolicy!"
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value("is_active", "bad_value")
    assert (
        str(err.value)
        == "'is_active' value needs to be of type int (not str) for PassthroughPolicy!"
    )
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value("is_enforced", "bad_value")
    assert (
        str(err.value)
        == "'is_enforced' value needs to be of type int (not str) for PassthroughPolicy!"
    )
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value("max_threads", "bad_value")
    assert (
        str(err.value)
        == "'max_threads' value needs to be of type int (not str) for PassthroughPolicy!"
    )

    with pytest.raises(InvalidPolicyConfig) as err:
        policy.get_config_value("does_not_exist")
    assert (
        str(err.value)
        == "'does_not_exist' is not a valid config for PassthroughPolicy!"
    )


class SomeParametrizedConfigPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return [
            AllocationPolicyConfig(
                name="my_config", description="", value_type=int, default=10
            ),
            AllocationPolicyConfig(
                name="my_param_config",
                description="",
                value_type=int,
                default=-1,
                param_types={"org": int, "ref": str},
            ),
        ]

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        raise

    def _update_quota_balance(
        self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
    ) -> None:
        pass


class TestAllocationPolicyLogs(TestCase):
    @pytest.mark.redis_db
    def test_bad_config_key_in_redis(self) -> None:
        policy = SomeParametrizedConfigPolicy(StorageKey("something"), [], {})
        set_config(
            key="something.SomeParametrizedConfigPolicy.my_bad_config.org:10,ref:ref",
            value=10,
            config_key=CAPMAN_HASH,
        )
        set_config(
            key="something.SomeParametrizedConfigPolicy.my_param_config.org:10",
            value=10,
            config_key=CAPMAN_HASH,
        )
        set_config(
            key="something.SomeParametrizedConfigPolicy.my_param_config.org:10,ref:ref,yeet:yeet",
            value=10,
            config_key=CAPMAN_HASH,
        )
        with self.assertLogs() as captured:
            configs = policy.get_current_configs()

        # the bad configs are not returned
        assert len(configs) == 4

        # the bad configs are logged
        assert len(captured.records) == 3
        logs = set([record.getMessage() for record in captured.records])
        assert logs == {
            "AllocationPolicy could not deserialize a key: something.SomeParametrizedConfigPolicy.my_bad_config.org:10,ref:ref",
            "AllocationPolicy could not deserialize a key: something.SomeParametrizedConfigPolicy.my_param_config.org:10",
            "AllocationPolicy could not deserialize a key: something.SomeParametrizedConfigPolicy.my_param_config.org:10,ref:ref,yeet:yeet",
        }


@pytest.fixture(scope="function")
def policy() -> AllocationPolicy:
    policy = SomeParametrizedConfigPolicy(StorageKey("something"), [], {})
    return policy


@pytest.mark.redis_db
def test_config_validation(policy: AllocationPolicy) -> None:
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value(
            config_key="my_config", value=10, params={"bad_param": 10}
        )
    assert (
        str(err.value)
        == "'my_config' takes no params for SomeParametrizedConfigPolicy!"
    )
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value(config_key="my_config", value="lol")
    assert (
        str(err.value)
        == "'my_config' value needs to be of type int (not str) for SomeParametrizedConfigPolicy!"
    )
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value(config_key="my_param_config", value=10)
    assert (
        str(err.value)
        == "'my_param_config' missing required parameters: {'org': 'int', 'ref': 'str'} for SomeParametrizedConfigPolicy!"
    )

    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config_value(
            config_key="my_param_config", value=10, params={"org": "lol", "ref": "test"}
        )
    assert (
        str(err.value)
        == "'my_param_config' parameter 'org' needs to be of type int (not str) for SomeParametrizedConfigPolicy!"
    )

    # strings should convert into ints if expected type is int
    policy.set_config_value(
        config_key="my_param_config", value=10, params={"org": "10", "ref": "test"}
    )
    assert policy.get_config_value("my_param_config", {"org": 10, "ref": "test"}) == 10


@pytest.mark.redis_db
def test_add_delete_config_value(policy: AllocationPolicy) -> None:
    """Test adding + resetting a simple config"""
    config_key = "my_config"

    policy.set_config_value(config_key=config_key, value=100)
    assert policy.get_config_value(config_key=config_key) == 100

    policy.delete_config_value(config_key=config_key)
    # back to default
    assert policy.get_config_value(config_key=config_key) == 10

    """Test adding + deleting an optional config"""
    config_key = "my_param_config"
    params = {"org": 10, "ref": "test"}

    policy.set_config_value(config_key=config_key, value=100, params=params)
    assert policy.get_config_value(config_key=config_key, params=params) == 100

    policy.delete_config_value(config_key=config_key, params=params)
    # back to default
    assert policy.get_config_value(config_key=config_key, params=params) == -1


@pytest.mark.redis_db
def test_default_config_overrides(policy: AllocationPolicy) -> None:
    assert policy.is_enforced == 1
    policy.set_config_value(config_key="is_enforced", value=0)
    assert policy.is_enforced == 0
    policy.set_config_value(config_key="is_enforced", value=1)
    assert policy.is_enforced == 1

    assert policy.is_active == 1
    policy.set_config_value(config_key="is_active", value=0)
    assert policy.is_active == 0
    policy.set_config_value(config_key="is_active", value=1)
    assert policy.is_active == 1

    assert policy.max_threads == 10
    policy.set_config_value(config_key="max_threads", value=4)
    assert policy.max_threads == 4
    policy.set_config_value(config_key="max_threads", value=10)
    assert policy.max_threads == 10


@pytest.mark.redis_db
def test_get_current_configs(policy: AllocationPolicy) -> None:
    assert len(policy_configs := policy.get_current_configs()) == 4
    assert all(
        config in policy_configs
        for config in [
            {
                "name": "my_config",
                "type": "int",
                "default": 10,
                "description": "",
                "value": 10,
                "params": {},
            },
            {
                "name": "is_active",
                "type": "int",
                "default": 1,
                "description": "Toggles whether or not this policy is active. If active, policy code will be excecuted. If inactive, the policy code will not run and the query will pass through.",
                "value": 1,
                "params": {},
            },
            {
                "name": "is_enforced",
                "type": "int",
                "default": 1,
                "description": "Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
                "value": 1,
                "params": {},
            },
            {
                "name": "max_threads",
                "type": "int",
                "default": 10,
                "description": "The max threads Clickhouse can use for the query.",
                "value": 10,
                "params": {},
            },
        ]
    )

    # add an instance of an optional config
    policy.set_config_value(
        config_key="my_param_config", value=100, params={"org": 10, "ref": "test"}
    )
    policy.set_config_value(config_key="is_enforced", value=0)
    policy.set_config_value(config_key="max_threads", value=4)
    assert len(policy_configs := policy.get_current_configs()) == 5
    assert {
        "name": "my_param_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 100,
        "params": {"org": 10, "ref": "test"},
    } in policy_configs
    assert {
        "name": "is_enforced",
        "type": "int",
        "default": 1,
        "description": "Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
        "value": 0,
        "params": {},
    } in policy_configs
    assert {
        "name": "max_threads",
        "type": "int",
        "default": 10,
        "description": "The max threads Clickhouse can use for the query.",
        "value": 4,
        "params": {},
    } in policy_configs
    assert policy.is_enforced == 0
    assert policy.max_threads == 4


@pytest.mark.redis_db
def test_default_config_override() -> None:
    policy = SomeParametrizedConfigPolicy(
        StorageKey("some_storage"), [], {"my_param_config": 420, "is_enforced": 0}
    )
    assert (
        policy.get_config_value(
            "my_param_config", params={"org": 1, "ref": "a"}, validate=True
        )
        == 420
    )
    assert policy.get_config_value("is_enforced") == 0


@pytest.mark.redis_db
def test_bad_defaults() -> None:
    with pytest.raises(ValueError):
        SomeParametrizedConfigPolicy(
            StorageKey("some_storage"), [], {"is_enforced": "0"}
        )
    with pytest.raises(ValueError):
        SomeParametrizedConfigPolicy(
            StorageKey("some_storage"), [], {"is_active": False}
        )
    with pytest.raises(ValueError):
        SomeParametrizedConfigPolicy(
            StorageKey("some_storage"), [], {"my_param_config": False}
        )


@pytest.mark.redis_db
def test_is_not_active() -> None:
    # active policy
    policy = BadlyWrittenAllocationPolicy(
        StorageKey("some_storage"),
        [],
        {"my_param_config": 420, "is_active": 1, "is_enforced": 0},
    )

    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    result_or_error = QueryResultOrError(
        query_result=QueryResult(
            result={"profile": {"bytes": 420}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
        error=None,
    )

    # Should error since private methods _get_quota_allowance and _update_quota_balance are called
    with pytest.raises(AttributeError):
        policy.get_quota_allowance(tenant_ids, "deadbeef")
    with pytest.raises(ValueError):
        policy.update_quota_balance(tenant_ids, "deadbeef", result_or_error)

    policy.set_config_value(config_key="is_active", value=0)  # make policy inactive

    # Should not error anymore since private methods are not called due to inactivity
    policy.get_quota_allowance(tenant_ids, "deadbeef")
    policy.update_quota_balance(tenant_ids, "deadbeef", result_or_error)


@pytest.mark.redis_db
def test_is_not_enforced() -> None:
    MAX_THREADS = 100
    reject_policy = RejectingEverythingAllocationPolicy(
        StorageKey("some_storage"),
        [],
        {
            "my_param_config": 420,
            "is_active": 1,
            "is_enforced": 1,
            "max_threads": MAX_THREADS,
        },
    )
    throttle_policy = ThrottleEverythingAllocationPolicy(
        StorageKey("some_storage"),
        [],
        {"is_active": 1, "is_enforced": 1, "max_threads": MAX_THREADS},
    )
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    assert not reject_policy.get_quota_allowance(tenant_ids, "deadbeef").can_run

    reject_policy.set_config_value(config_key="is_enforced", value=0)
    # policy not enforced so we don't reject the query
    reject_policy.get_quota_allowance(tenant_ids, "deadbeef")

    assert throttle_policy.get_quota_allowance(tenant_ids, "deadbeef").max_threads == 1
    throttle_policy.set_config_value(config_key="is_enforced", value=0)
    assert (
        throttle_policy.get_quota_allowance(tenant_ids, "deadbeef").max_threads
        == MAX_THREADS
    )

    rejected_metrics = get_recorded_metric_calls(
        "increment", "allocation_policy.db_request_rejected"
    )
    assert len(rejected_metrics) == 2
    assert (
        rejected_metrics[0].tags["policy_class"]
        == "RejectingEverythingAllocationPolicy"
    )
    assert rejected_metrics[0].tags["is_enforced"] == "True"
    assert rejected_metrics[1].tags["is_enforced"] == "False"
    throttled_metrics = get_recorded_metric_calls(
        "increment", "allocation_policy.db_request_throttled"
    )
    assert len(throttled_metrics) == 2, throttled_metrics
    assert (
        throttled_metrics[0].tags["policy_class"]
        == "ThrottleEverythingAllocationPolicy"
    )
    assert throttled_metrics[0].tags["is_enforced"] == "True"
    assert throttled_metrics[1].tags["is_enforced"] == "False"


@pytest.mark.redis_db
def test_configs_with_delimiter_values() -> None:
    # test that configs with dots can be stored and read
    policy = SomeParametrizedConfigPolicy(StorageKey("something"), [], {})
    policy.set_config_value("my_param_config", 5, {"ref": "a,::.b.c", "org": 1})
    configs = policy.get_current_configs()
    print(configs)
    assert {
        "name": "my_param_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 5,
        "params": {"org": 1, "ref": "a,::.b.c"},
    } in configs


def test_cannot_use_escape_sequences() -> None:
    policy = SomeParametrizedConfigPolicy(StorageKey("something"), [], {})
    with pytest.raises(InvalidPolicyConfig):
        policy.set_config_value(
            "my_param_config", 5, {"ref": "a__dot_literal__.b.c", "org": 1}
        )
