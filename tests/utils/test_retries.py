from snuba.utils.clock import TestingClock
from snuba.utils.retries import BasicRetryPolicy, RetryException, constant_delay


class CustomException(Exception):
    pass


def test_basic_retry_policy_no_delay():

    value = object()

    def good_function():
        good_function.call_count += 1
        return value

    good_function.call_count = 0

    def flaky_function():
        flaky_function.call_count += 1
        if flaky_function.call_count % 2 == 0:
            return value
        else:
            raise CustomException()

    flaky_function.call_count = 0

    def bad_function():
        bad_function.call_count += 1
        raise CustomException()

    bad_function.call_count = 0

    clock = TestingClock()

    policy = BasicRetryPolicy(3, clock=clock)

    assert policy.call(good_function) is value
    assert good_function.call_count == 1
    assert clock.time() == 0

    assert policy.call(flaky_function) is value
    assert flaky_function.call_count == 2
    assert clock.time() == 0

    try:
        policy.call(bad_function)
    except Exception as e:
        assert isinstance(e, RetryException)
        assert isinstance(e.__cause__, CustomException)
        assert bad_function.call_count == 3
        assert clock.time() == 0


def test_basic_retry_policy_with_delay():

    value = object()

    def good_function():
        good_function.call_count += 1
        return value

    good_function.call_count = 0

    def flaky_function():
        flaky_function.call_count += 1
        if flaky_function.call_count % 2 == 0:
            return value
        else:
            raise CustomException()

    flaky_function.call_count = 0

    def bad_function():
        bad_function.call_count += 1
        raise CustomException()

    bad_function.call_count = 0

    clock = TestingClock()
    policy = BasicRetryPolicy(3, constant_delay(1), clock=clock)
    assert policy.call(good_function) is value
    assert good_function.call_count == 1
    assert clock.time() == 0

    clock = TestingClock()
    policy = BasicRetryPolicy(3, constant_delay(1), clock=clock)
    assert policy.call(flaky_function) is value
    assert flaky_function.call_count == 2
    assert clock.time() == 1  # one retry

    clock = TestingClock()
    policy = BasicRetryPolicy(3, constant_delay(1), clock=clock)
    try:
        policy.call(bad_function)
    except Exception as e:
        assert isinstance(e, RetryException)
        assert isinstance(e.__cause__, CustomException)
        assert bad_function.call_count == 3
        assert clock.time() == 2  # two retries
