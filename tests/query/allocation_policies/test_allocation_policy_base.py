from __future__ import annotations

from unittest import TestCase, mock

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    CAPMAN_HASH,
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicy,
    AllocationPolicyConfig,
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


def test_raises_on_false_can_run() -> None:
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

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        raise

    def _update_quota_balance(
        self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
    ) -> None:
        pass


class TestAllocationPolicyLogs(TestCase):
    @pytest.mark.redis_db
    def test_bad_config_key_in_redis(self) -> None:
        policy = SomeParametrizedConfigPolicy(StorageKey("something"), [])
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
        assert len(configs) == 3

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
    policy = SomeParametrizedConfigPolicy(StorageKey("something"), [])
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
def test_get_current_configs(policy: AllocationPolicy) -> None:
    assert len(policy_configs := policy.get_current_configs()) == 3
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
                "description": "Whether or not this policy is active.",
                "value": 1,
                "params": {},
            },
            {
                "name": "is_enforced",
                "type": "int",
                "default": 1,
                "description": "Whether or not this policy is enforced.",
                "value": 1,
                "params": {},
            },
        ]
    )

    # add an instance of an optional config
    policy.set_config_value(
        config_key="my_param_config", value=100, params={"org": 10, "ref": "test"}
    )
    assert len(policy_configs := policy.get_current_configs()) == 4
    assert {
        "name": "my_param_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 100,
        "params": {"org": 10, "ref": "test"},
    } in policy_configs


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
