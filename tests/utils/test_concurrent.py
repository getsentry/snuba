import threading
import time

import pytest
from concurrent.futures import TimeoutError

from snuba.utils.concurrent import Synchronized, execute


def test_execute() -> None:
    assert execute(threading.current_thread).result() != threading.current_thread()

    with pytest.raises(ZeroDivisionError):
        assert execute(lambda: 1 / 0).result()

    with pytest.raises(TimeoutError):
        assert execute(lambda: time.sleep(10), daemon=True).result(timeout=0)


def test_synchronized() -> None:
    value = object()
    wrapper = Synchronized(value)

    with wrapper.get() as wrapped:
        assert wrapped is value

    wrapper.set(object())

    with wrapper.get() as wrapped:
        assert wrapped is not value

    wrapper.set(value)

    with wrapper.get() as wrapped:
        assert wrapped is value
