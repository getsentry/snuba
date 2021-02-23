from snuba.utils.clock import TestingClock
from snuba.utils.metrics.timer import Timer
from tests.backends.metrics import TestingMetricsBackend, Timing


def test_timer() -> None:
    time = TestingClock()

    t = Timer("timer", clock=time)
    time.sleep(10.0)
    t.mark("thing1")
    time.sleep(10.0)
    t.mark("thing2")
    assert t.finish() == {
        "timestamp": 0.0,
        "duration_ms": (10.0 + 10.0) * 1000,
        "marks_ms": {"thing1": 10.0 * 1000, "thing2": 10.0 * 1000},
    }

    # Test that we can add more time under the same marks and the time will
    # be cumulatively added under those keys.
    time.sleep(10.0)
    t.mark("thing1")
    time.sleep(10.0)
    t.mark("thing2")
    assert t.finish() == {
        "timestamp": 0.0,
        "duration_ms": (10.0 + 10.0) * 2 * 1000,
        "marks_ms": {"thing1": 10.0 * 2 * 1000, "thing2": 10.0 * 2 * 1000},
    }


def test_timer_send_metrics() -> None:
    backend = TestingMetricsBackend()

    time = TestingClock()

    t = Timer("timer", clock=time)
    time.sleep(10)
    t.mark("thing1")
    time.sleep(10)
    t.mark("thing2")
    t.send_metrics_to(
        backend, tags={"key": "value"}, mark_tags={"mark-key": "mark-value"}
    )

    assert backend.calls == [
        Timing("timer", (10.0 + 10.0) * 1000, {"key": "value"}),
        Timing("timer.thing1", 10.0 * 1000, {"mark-key": "mark-value"}),
        Timing("timer.thing2", 10.0 * 1000, {"mark-key": "mark-value"}),
    ]


def test_timer_send_metrics_fake_times() -> None:
    backend = TestingMetricsBackend()

    time = TestingClock(100)

    t = Timer("timer", clock=time, init_time=5.0)
    time.sleep(10)
    t.mark("thing1", 10.0)
    time.sleep(10)
    t.mark("thing2", 11.0)
    t.send_metrics_to(backend)

    assert backend.calls == [
        Timing("timer", (11.0 - 5.0) * 1000, None),
        Timing("timer.thing1", 5.0 * 1000, None),
        Timing("timer.thing2", 1000, None),
    ]
