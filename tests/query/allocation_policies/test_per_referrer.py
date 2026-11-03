import pytest

from snuba.query.allocation_policies import QueryResultOrError
from snuba.query.allocation_policies.per_referrer import ReferrerGuardRailPolicy
from snuba.web import QueryResult
from tests.configs.component_config import set_component_config

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
                "storage_key": "metrics_counters",
            }
        )

        set_component_config(policy, "default_concurrent_request_per_referrer", 2)
        policy.get_quota_allowance(tenant_ids={"referrer": "statistical_detectors"}, query_id="1")

        policy.get_quota_allowance(tenant_ids={"referrer": "statistical_detectors"}, query_id="2")

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
                "storage_key": "metrics_counters",
            }
        )

        set_component_config(policy, "default_concurrent_request_per_referrer", 4)
        set_component_config(policy, "requests_throttle_divider", 2)
        set_component_config(policy, "threads_throttle_divider", 2)
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
                "storage_key": "metrics_counters",
            }
        )
        set_component_config(
            policy,
            "referrer_concurrent_override",
            0,
            {"referrer": "statistical_detectors"},
        )

        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "statistical_detectors"}, query_id="2"
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0

    @pytest.mark.redis_db
    def test_respects_max_threads_config(self) -> None:
        policy = ReferrerGuardRailPolicy.from_kwargs(
            **{
                "storage_key": "metrics_counters",
            }
        )
        set_component_config(policy, "max_threads", 4)
        assert (
            policy.get_quota_allowance(
                tenant_ids={"referrer": "statistical_detectors"}, query_id="1"
            ).max_threads
            == 4
        )

    @pytest.mark.redis_db
    def test_throttle_threshold_follows_referrer_override(self) -> None:
        policy = ReferrerGuardRailPolicy.from_kwargs(
            **{
                "storage_key": "metrics_counters",
            }
        )
        set_component_config(policy, "max_threads", 4)
        set_component_config(policy, "requests_throttle_divider", 2)
        set_component_config(policy, "referrer_concurrent_override", 4, {"referrer": "r"})
        tenant_ids: dict[str, str | int] = {"referrer": "r"}

        # Threshold is 4 // 2 = 2, not the default 100 // 2 = 50.
        first = policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="1")
        second = policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="2")
        third = policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="3")
        assert not first.is_throttled and not second.is_throttled
        assert third.is_throttled and third.max_threads == 2
        assert third.throttle_threshold == 2
