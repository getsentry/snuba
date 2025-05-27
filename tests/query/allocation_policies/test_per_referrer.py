import pytest

from snuba.query.allocation_policies import QueryResultOrError
from snuba.query.allocation_policies.per_referrer import ReferrerGuardRailPolicy
from snuba.web import QueryResult

_RESULT_SUCCESS = QueryResultOrError(
    QueryResult(
        result={"profile": {"bytes": 42069}},
        extra={"stats": {}, "sql": "", "experiments": {}},
    ),
    error=None,
)


class TestPerReferrerPolicy:
    @pytest.mark.redis_db
    def test_policy_pass_basic(self):
        policy = ReferrerGuardRailPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
            }
        )

        policy.set_config_value("default_concurrent_request_per_referrer", 2)
        policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="1"
        )

        policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="2"
        )

        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="3"
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0
        # clean up the failed request
        policy.update_quota_balance(
            tenant_ids={"referrer": "statistical_detectors"},
            query_id="3",
            result_or_error=_RESULT_SUCCESS,
        )
        # finish first request, now should be room for one more
        policy.update_quota_balance(
            tenant_ids={"referrer": "statistical_detectors"},
            query_id="1",
            result_or_error=_RESULT_SUCCESS,
        )
        assert policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="4"
        ).can_run

    @pytest.mark.redis_db
    def test_throttle(self) -> None:
        policy = ReferrerGuardRailPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
            }
        )

        policy.set_config_value("default_concurrent_request_per_referrer", 4)
        policy.set_config_value("requests_throttle_divider", 2)
        policy.set_config_value("threads_throttle_divider", 2)
        first_quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="1"
        )
        assert first_quota_allowance.max_threads == policy.max_threads

        second_quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="2"
        )
        assert second_quota_allowance.max_threads == policy.max_threads

        third_quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="3"
        )
        assert third_quota_allowance.max_threads == policy.max_threads // 2
        assert third_quota_allowance.can_run

    @pytest.mark.redis_db
    def test_override(self):
        policy = ReferrerGuardRailPolicy.from_kwargs(
            **{
                "storage_key": "generic_metrics_distributions",
                "required_tenant_types": ["referrer"],
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
