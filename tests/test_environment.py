from arroyo.processing.strategies.run_task_with_multiprocessing import (
    ChildProcessTerminated,
)

from snuba.environment import before_send
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.web.rpc.common.exceptions import RPCAllocationPolicyException


def _hint_for(exc: BaseException) -> dict:
    return {"exc_info": (type(exc), exc, exc.__traceback__)}


def test_before_send_passes_through_without_exc_info() -> None:
    event = {"message": "hello"}
    assert before_send(dict(event), {}) == event


def test_before_send_passes_through_unrelated_exception() -> None:
    try:
        raise ValueError("a real bug")
    except ValueError as err:
        assert before_send({"message": "boom"}, _hint_for(err)) is not None


def test_before_send_drops_child_process_terminated() -> None:
    """
    A consumer multiprocessing worker dying (SIGCHLD) is logged at ERROR and
    re-raised by arroyo; it recovers on restart and is not an actionable issue.
    """
    try:
        raise ChildProcessTerminated(17)
    except ChildProcessTerminated as err:
        assert (
            before_send({"message": "Caught exception, shutting down..."}, _hint_for(err)) is None
        )


def test_before_send_drops_child_process_terminated_in_cause_chain() -> None:
    try:
        try:
            raise ChildProcessTerminated(17)
        except ChildProcessTerminated as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as err:
        assert before_send({"message": "wrapped"}, _hint_for(err)) is None


def test_before_send_keeps_event_when_noise_is_in_suppressed_context() -> None:
    """
    `raise ... from None` keeps the prior exception on __context__ but suppresses
    it; we must not follow it, otherwise a legitimate error would be dropped just
    because a noise type sits in its (explicitly suppressed) context.
    """
    try:
        try:
            raise ChildProcessTerminated(17)
        except ChildProcessTerminated:
            raise ValueError("a real bug") from None
    except ValueError as err:
        assert err.__suppress_context__ is True
        assert isinstance(err.__context__, ChildProcessTerminated)
        assert before_send({"message": "a real bug"}, _hint_for(err)) is not None


def test_before_send_drops_allocation_policy_violations() -> None:
    try:
        raise AllocationPolicyViolations("rejected")
    except AllocationPolicyViolations as err:
        assert before_send({"message": "rejected"}, _hint_for(err)) is None


def test_before_send_drops_rpc_allocation_policy_exception() -> None:
    try:
        raise RPCAllocationPolicyException("rejected", {})
    except RPCAllocationPolicyException as err:
        assert before_send({"message": "rejected"}, _hint_for(err)) is None


def test_before_send_handles_none_exc_value() -> None:
    event = {"message": "no exc"}
    assert before_send(dict(event), {"exc_info": (None, None, None)}) == event


def test_before_send_terminates_on_cyclic_cause_chain() -> None:
    # Defensive: a self-referential context must not loop forever.
    err = ValueError("self")
    try:
        raise err
    except ValueError:
        err.__context__ = err
        assert before_send({"message": "cycle"}, _hint_for(err)) is not None
