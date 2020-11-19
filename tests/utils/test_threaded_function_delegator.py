import threading
from typing import Any, List, Tuple
from unittest.mock import Mock

from snuba.utils.threaded_function_delegator import ThreadedFunctionDelegator


def test() -> None:
    condition = threading.Condition()

    condition_add_delay = threading.Condition()

    def artificial_delay() -> int:
        with condition_add_delay:
            condition_add_delay.wait()
        return 2

    callables = {
        "one": Mock(return_value=1),
        "two": Mock(side_effect=artificial_delay),
        "three": Mock(return_value=3),
    }

    def selector_func(_: int) -> Tuple[str, List[str]]:
        return ("one", ["two"])

    def callback_func(args: List[Tuple[str, int]]) -> None:
        assert args == [("one", 1), ("two", 2)]
        with condition:
            condition.notify()

    delegator = ThreadedFunctionDelegator[Any, int](
        callables=callables, selector_func=selector_func, callback_func=callback_func,
    )

    result = delegator.execute(5)

    # Allow the second callable to return
    with condition_add_delay:
        condition_add_delay.notify()

    # Wait until the callback has finished running
    with condition:
        condition.wait()

    assert result == 1

    assert callables["one"].call_count == 1
    assert callables["two"].call_count == 1
    assert callables["three"].call_count == 0
