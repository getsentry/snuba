import pytest

from snuba.utils.clock import TestingClock
from snuba.utils.retries import BasicRetryPolicy, RetryException, constant_delay


def test_basic_retry_policy_no_delay():

    value = object()

    def good_function():
        good_function.call_count += 1
        return value

    good_function.call_count = 0

    class CustomException(Exception):
        pass

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


@pytest.mark.parametrize("delay", [1, constant_delay(1)])
def test_basic_retry_policy_with_delay(delay):

    value = object()

    def good_function():
        good_function.call_count += 1
        return value

    good_function.call_count = 0

    class CustomException(Exception):
        pass

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
    policy = BasicRetryPolicy(3, delay, clock=clock)
    assert policy.call(good_function) is value
    assert good_function.call_count == 1
    assert clock.time() == 0

    clock = TestingClock()
    policy = BasicRetryPolicy(3, delay, clock=clock)
    assert policy.call(flaky_function) is value
    assert flaky_function.call_count == 2
    assert clock.time() == 1  # one retry

    clock = TestingClock()
    policy = BasicRetryPolicy(3, delay, clock=clock)
    try:
        policy.call(bad_function)
    except Exception as e:
        assert isinstance(e, RetryException)
        assert isinstance(e.__cause__, CustomException)
        assert bad_function.call_count == 3
        assert clock.time() == 2  # two retries


def test_basic_retry_policy_with_supression():
    class ExpectedError(Exception):
        pass

    class UnexpectedError(Exception):
        pass

    def function():
        function.call_count += 1
        if function.call_count % 2 == 0:
            raise UnexpectedError()
        else:
            raise ExpectedError()

    function.call_count = 0

    def suppression_test(exception: Exception) -> bool:
        return isinstance(exception, ExpectedError)

    clock = TestingClock()
    policy = BasicRetryPolicy(
        3, constant_delay(1), suppression_test=suppression_test, clock=clock
    )
    try:
        policy.call(function)
    except UnexpectedError:
        assert function.call_count == 2
        assert clock.time() == 1
