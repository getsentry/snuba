import pytest

from snuba.query.allocation_policies import AllocationPolicyViolation
from snuba.query.allocation_policies.cross_org import CrossOrgQueryAllocationPolicy


class TestCrossOrgQueryAllocationPolicy:
    def test_policy_pass_basic(self):
        policy = CrossOrgQueryAllocationPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
                "cross_org_referrer_limits": {
                    "statistical_detectors": {
                        "concurrent_limit": 2,
                        "max_threads": 1,
                    },
                },
            }
        )
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "some_referrer"}, query_id="1"
            ).can_run
            is True
        )
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "some_referrer"}, query_id="2"
            ).max_threads
            == policy.max_threads
        )
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "statistical_detectors"}, query_id="3"
            ).can_run
            is True
        )
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "statistical_detectors"}, query_id="4"
            ).max_threads
            == 1
        )

        with pytest.raises(AllocationPolicyViolation):
            policy.get_quota_allowance(
                tenant_ids={"referrer": "statistical_detectors"}, query_id="5"
            )

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
