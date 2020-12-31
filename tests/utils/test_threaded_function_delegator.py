import threading
from typing import Any, List, Tuple
from unittest.mock import Mock, call, ANY

from snuba.utils.threaded_function_delegator import Result, ThreadedFunctionDelegator


def test() -> None:
    callback_done = threading.Event()

    result_received = threading.Event()

    callables = {
        "one": Mock(return_value=1),
        "two": Mock(return_value=2),
        "three": Mock(return_value=3),
    }

    def selector_func(_: int) -> Tuple[str, List[str]]:
        return ("one", ["two"])

    def callback_func(args: List[Tuple[str, int]]) -> None:
        assert result_received.wait(
            timeout=5
        ), "Timeout while waiting for the main thread."
        callback_done.set()

    mock_callback = Mock(side_effect=callback_func)

    delegator = ThreadedFunctionDelegator[Any, int](
        callables=callables, selector_func=selector_func, callback_func=mock_callback,
    )

    result = delegator.execute(5)

    # Allow the callback to return
    result_received.set()

    # Wait until the callback has finished running
    assert callback_done.wait(
        timeout=5
    ), "Timeout while waiting for the callback to complete"

    assert result == 1

    assert callables["one"].call_count == 1
    assert callables["two"].call_count == 1
    assert callables["three"].call_count == 0
    assert mock_callback.call_args == call(
        [Result("one", 1, ANY), Result("two", 2, ANY)]
    )
