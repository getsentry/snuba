from typing import Any, Optional
from unittest import mock

import pytest

from snuba.utils.clock import TestingClock
from snuba.utils.retries import BasicRetryPolicy, RetryException, constant_delay

value = object()


def _good_function() -> Any:
    return value


good_function = mock.Mock(side_effect=_good_function)


class CustomException(Exception):
    pass


def _flaky_function() -> Optional[Any]:
    if flaky_function.call_count % 2 == 0:
        return value
    else:
        raise CustomException()


flaky_function = mock.Mock(side_effect=_flaky_function)


def _bad_function() -> None:
    raise CustomException()


bad_function = mock.Mock(side_effect=_bad_function)


def setup_function() -> None:
    good_function.reset_mock()
    flaky_function.reset_mock()
    bad_function.reset_mock()


def test_basic_retry_policy_no_delay() -> None:

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
def test_basic_retry_policy_with_delay(delay: int) -> None:
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


def test_basic_retry_policy_with_supression() -> None:
    class ExpectedError(Exception):
        pass

    class UnexpectedError(Exception):
        pass

    def _test_function() -> None:
        if test_function.call_count % 2 == 0:
            raise UnexpectedError()
        else:
            raise ExpectedError()

    test_function = mock.Mock(side_effect=_test_function)

    def suppression_test(exception: Exception) -> bool:
        return isinstance(exception, ExpectedError)

    clock = TestingClock()
    policy = BasicRetryPolicy(
        3, constant_delay(1), suppression_test=suppression_test, clock=clock
    )
    try:
        policy.call(test_function)
    except UnexpectedError:
        assert test_function.call_count == 2
        assert clock.time() == 1
