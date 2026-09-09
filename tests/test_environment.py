from collections.abc import Callable
from typing import Any

import pytest
import sentry_sdk
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    ChildProcessTerminated,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisClusterException
from redis.exceptions import TimeoutError as RedisTimeoutError
from sentry_sdk.types import Event, Hint

from snuba import settings
from snuba.environment import before_send, traces_sampler
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.web.rpc.common.exceptions import RPCAllocationPolicyException


def _hint_for(exc: BaseException) -> Hint:
    return {"exc_info": (type(exc), exc, exc.__traceback__)}


def test_before_send_passes_through_without_exc_info() -> None:
    event: Event = {"message": "hello"}
    assert before_send(event, {}) is event


def test_before_send_passes_through_unrelated_exception() -> None:
    event: Event = {"message": "boom"}
    try:
        raise ValueError("a real bug")
    except ValueError as err:
        assert before_send(event, _hint_for(err)) is event


def test_before_send_drops_child_process_terminated() -> None:
    """
    A consumer multiprocessing worker dying (SIGCHLD) is logged at ERROR and
    re-raised by arroyo; it recovers on restart and is not an actionable issue.
    """
    event: Event = {"message": "Caught exception, shutting down..."}
    try:
        raise ChildProcessTerminated(17)
    except ChildProcessTerminated as err:
        assert before_send(event, _hint_for(err)) is None


def test_before_send_drops_child_process_terminated_in_cause_chain() -> None:
    event: Event = {"message": "wrapped"}
    try:
        try:
            raise ChildProcessTerminated(17)
        except ChildProcessTerminated as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as err:
        assert before_send(event, _hint_for(err)) is None


def test_before_send_keeps_event_when_noise_is_in_suppressed_context() -> None:
    """
    `raise ... from None` keeps the prior exception on __context__ but suppresses
    it; we must not follow it, otherwise a legitimate error would be dropped just
    because a noise type sits in its (explicitly suppressed) context.
    """
    event: Event = {"message": "a real bug"}
    try:
        try:
            raise ChildProcessTerminated(17)
        except ChildProcessTerminated:
            raise ValueError("a real bug") from None
    except ValueError as err:
        assert err.__suppress_context__ is True
        assert isinstance(err.__context__, ChildProcessTerminated)
        assert before_send(event, _hint_for(err)) is event


def test_before_send_drops_allocation_policy_violations() -> None:
    event: Event = {"message": "rejected"}
    try:
        raise AllocationPolicyViolations("rejected")
    except AllocationPolicyViolations as err:
        assert before_send(event, _hint_for(err)) is None


def test_before_send_drops_rpc_allocation_policy_exception() -> None:
    event: Event = {"message": "rejected"}
    try:
        raise RPCAllocationPolicyException("rejected", {})
    except RPCAllocationPolicyException as err:
        assert before_send(event, _hint_for(err)) is None


def test_before_send_drops_redis_cluster_connectivity_exception() -> None:
    event: Event = {"message": "redis unreachable"}
    try:
        try:
            raise RedisTimeoutError("Timeout connecting to server")
        except RedisTimeoutError as cause:
            raise RedisClusterException(
                "Redis Cluster cannot be connected. Please provide at least "
                f"one reachable node: {cause}"
            ) from cause
    except RedisClusterException as err:
        assert before_send(event, _hint_for(err)) is None


def test_before_send_keeps_unrelated_redis_cluster_exception() -> None:
    event: Event = {"message": "redis programming error"}
    try:
        raise RedisClusterException("method eval() is not implemented")
    except RedisClusterException as err:
        assert before_send(event, _hint_for(err)) is event


def test_before_send_keeps_redis_cluster_misconfiguration_exception() -> None:
    event: Event = {"message": "redis misconfigured"}
    try:
        try:
            raise RedisClusterException("Cluster mode is not enabled on this node")
        except RedisClusterException as cause:
            raise RedisClusterException(
                "Redis Cluster cannot be connected. Please provide at least "
                f"one reachable node: {cause}"
            ) from cause
    except RedisClusterException as err:
        assert before_send(event, _hint_for(err)) is event


def test_before_send_keeps_redis_connection_error_outside_cluster_exception() -> None:
    event: Event = {"message": "redis connection error"}
    try:
        raise RedisConnectionError("Error connecting to redis")
    except RedisConnectionError as err:
        assert before_send(event, _hint_for(err)) is event


def test_before_send_handles_none_exc_value() -> None:
    event: Event = {"message": "no exc"}
    hint: Hint = {"exc_info": (None, None, None)}
    assert before_send(event, hint) is event


def test_before_send_terminates_on_cyclic_cause_chain() -> None:
    # Defensive: a self-referential context must not loop forever.
    event: Event = {"message": "cycle"}
    err = ValueError("self")
    try:
        raise err
    except ValueError:
        err.__context__ = err
        assert before_send(event, _hint_for(err)) is event


@pytest.fixture
def sentry_environment(monkeypatch: pytest.MonkeyPatch) -> Callable[[str | None], None]:
    def _set(environment: str | None) -> None:
        monkeypatch.setattr(sentry_sdk.get_client(), "options", {"environment": environment})

    _set(None)
    return _set


@pytest.fixture
def sample_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SENTRY_TRACE_SAMPLE_RATE", 0.5)
    monkeypatch.setattr(settings, "SENTRY_HEALTH_CHECK_TRACE_SAMPLE_RATE", 0.01)


def _request(path: str, parent_sampled: bool | None = None) -> dict[str, Any]:
    return {
        "transaction_context": {"name": "generic WSGI request"},
        "parent_sampled": parent_sampled,
        "wsgi_environ": {"PATH_INFO": path},
    }


@pytest.mark.usefixtures("sentry_environment", "sample_rates")
def test_traces_sampler_inherits_parent_decision() -> None:
    assert traces_sampler(_request("/health", parent_sampled=True)) == 1.0
    assert traces_sampler(_request("/query", parent_sampled=False)) == 0.0


@pytest.mark.usefixtures("sentry_environment", "sample_rates")
def test_traces_sampler_uses_base_rate_for_root_requests() -> None:
    assert traces_sampler(_request("/query")) == 0.5
    assert traces_sampler(_request("/rpc/EndpointTraceItemTable/v1")) == 0.5


@pytest.mark.usefixtures("sentry_environment", "sample_rates")
def test_traces_sampler_uses_health_check_rate() -> None:
    assert traces_sampler(_request("/health")) == 0.01
    assert traces_sampler(_request("/health/")) == 0.01
    assert traces_sampler(_request("/health_envoy")) == 0.01
    assert traces_sampler(_request("/healthy")) == 0.5


@pytest.mark.usefixtures("sample_rates")
def test_traces_sampler_uses_base_rate_without_wsgi_environ(
    sentry_environment: Callable[[str | None], None],
) -> None:
    sentry_environment("us")
    assert traces_sampler({"transaction_context": {"name": "[cli init] api"}}) == 0.5


@pytest.mark.usefixtures("sample_rates")
@pytest.mark.parametrize("environment", ["dev", "local-simon", "test", "qa-eu", "debug"])
def test_traces_sampler_samples_everything_in_development_environments(
    sentry_environment: Callable[[str | None], None], environment: str
) -> None:
    sentry_environment(environment)
    assert traces_sampler(_request("/health")) == 1.0
    assert traces_sampler(_request("/query")) == 1.0


@pytest.mark.usefixtures("sample_rates")
@pytest.mark.parametrize("environment", ["us", "de", "s4s2", "production"])
def test_traces_sampler_keeps_rates_in_production_environments(
    sentry_environment: Callable[[str | None], None], environment: str
) -> None:
    sentry_environment(environment)
    assert traces_sampler(_request("/query")) == 0.5
