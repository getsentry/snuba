import threading
from typing import Any, List, Tuple
from unittest.mock import Mock

from snuba.utils.threaded_function_delegator import ThreadedFunctionDelegator


def test() -> None:
    cv = threading.Condition()

    callables = {
        "one": Mock(return_value=1),
        "two": Mock(return_value=2),
        "three": Mock(return_value=3),
    }

    def selector_func(_: int) -> Tuple[str, List[str]]:
        return ("one", ["two"])

    def callback_func(args: List[Tuple[str, int]]) -> None:
        with cv:
            assert args == [("one", 1), ("two", 2)]
            cv.notify()

    delegator = ThreadedFunctionDelegator[Any, int](
        callables=callables, selector_func=selector_func, callback_func=callback_func,
    )

    with cv:
        result = delegator.execute(5)
        cv.wait()

    assert result == 1

    assert callables["one"].call_count == 1
    assert callables["two"].call_count == 1
    assert callables["three"].call_count == 0
