import pytest

from snuba.query.allocation_policies import InvalidPolicyConfig, QueryResultOrError
from snuba.query.allocation_policies.cross_org import CrossOrgQueryAllocationPolicy
from snuba.web import QueryResult

_RESULT_SUCCESS = QueryResultOrError(
    QueryResult(
        result={"profile": {"bytes": 42069}},
        extra={"stats": {}, "sql": "", "experiments": {}},
    ),
    error=None,
)


class TestCrossOrgQueryAllocationPolicy:
    @pytest.mark.redis_db
    def test_policy_pass_basic(self):
        policy = CrossOrgQueryAllocationPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 1,
                        "max_threads": 1,
                    },
                },
            }
        )
        unimportant_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "some_referrer"}, query_id="1"
        )
        assert unimportant_allowance.can_run is True
        assert unimportant_allowance.max_threads == 10
        assert unimportant_allowance.explanation == {
            "reason": "pass_through",
            "storage_key": "StorageKey.GENERIC_METRICS_DISTRIBUTIONS",
        }
        cross_org_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="2"
        )
        assert cross_org_allowance.can_run is True
        assert cross_org_allowance.max_threads == 1

        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="3"
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0
        policy.update_quota_balance(
            tenant_ids={"referrer": "statistical_detectors"},
            query_id="2",
            result_or_error=_RESULT_SUCCESS,
        )
        assert policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="3"
        ).can_run

    @pytest.mark.parametrize(
        "config",
        [
            {
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "max_threads": 1,
                    },
                },
            },
            {
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 2,
                    },
                },
            },
            {
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {"max_threads": {}, "concurrent_limit": 2},
                },
            },
        ],
    )
    def test_bad_config(self, config) -> None:
        with pytest.raises(ValueError):
            CrossOrgQueryAllocationPolicy.from_kwargs(**config)

    @pytest.mark.redis_db
    def test_override(self):
        policy = CrossOrgQueryAllocationPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 1,
                        "max_threads": 1,
                    },
                },
            }
        )
        policy.set_config_value(
            "referrer_max_threads_override",
            2,
            {"referrer": "statistical_detectors"},
        )
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "statistical_detectors"}, query_id="1"
            ).max_threads
            == 2
        )
        policy.update_quota_balance(
            tenant_ids={"referrer": "statistical_detectors"},
            query_id="1",
            result_or_error=_RESULT_SUCCESS,
        )
        policy.set_config_value(
            "referrer_concurrent_override",
            0,
            {"referrer": "statistical_detectors"},
        )

        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="2"
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0

    @pytest.mark.redis_db
    def test_override_unregistered_referrer(self):
        # overrides of unregistered referrers should not be allowed
        policy = CrossOrgQueryAllocationPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 1,
                        "max_threads": 1,
                    },
                },
            }
        )
        with pytest.raises(InvalidPolicyConfig):
            policy.set_config_value(
                "referrer_concurrent_override",
                6,
                {"referrer": ""},
            )
        # this referrer is registered so that's fine
        policy.set_config_value(
            "referrer_concurrent_override",
            6,
            {"referrer": "statistical_detectors"},
        )
        # can still set regular configs
        policy.set_config_value("is_enforced", False)
        assert not policy.is_enforced
        policy.set_config_value("is_enforced", True)
        assert policy.is_enforced

    @pytest.mark.redis_db
    def test_throttle_cross_org_query_with_unregistered_referrer(self):
        policy = CrossOrgQueryAllocationPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 1,
                        "max_threads": 1,
                    },
                },
            }
        )
        allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "unregistered", "cross_org_query": 1},
            query_id="1",
        )
        assert allowance.can_run is True
        assert allowance.max_threads == 1

        allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "unregistered", "cross_org_query": 1},
            query_id="2",
        )
        assert not allowance.can_run
        assert allowance.explanation["cross_org_query"] == "This referrer is not registered for the current storage generic_metrics_distributions, if you want to increase its limits, register it in the yaml of the CrossOrgQueryAllocationPolicy"  # type: ignore
