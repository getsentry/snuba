from snuba.utils.metrics.clock import TestingClock
from snuba.utils.metrics.timer import Timer


def test_timer():
    time = TestingClock()

    t = Timer("timer", clock=time)
    time.sleep(10)
    t.mark("thing1")
    time.sleep(10)
    t.mark("thing2")
    assert t.finish() == {
        'timestamp': 0.0,
        'duration_ms': (10.0 + 10.0) * 1000,
        'marks_ms': {
            'thing1': 10.0 * 1000,
            'thing2': 10.0 * 1000,
        }
    }

    # Test that we can add more time under the same marks and the time will
    # be cumulatively added under those keys.
    time.sleep(10.0)
    t.mark("thing1")
    time.sleep(10.0)
    t.mark("thing2")
    assert t.finish() == {
        'timestamp': 0.0,
        'duration_ms': (10.0 + 10.0) * 2 * 1000,
        'marks_ms': {
            'thing1': 10.0 * 2 * 1000,
            'thing2': 10.0 * 2 * 1000,
        }
    }
