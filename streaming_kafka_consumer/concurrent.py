from __future__ import annotations

from concurrent.futures import Future
from contextlib import contextmanager
from threading import Lock, Thread
from typing import Callable, Generic, Iterator, Optional, TypeVar

T = TypeVar("T")


def execute(
    function: Callable[[], T], name: Optional[str] = None, daemon: Optional[bool] = None
) -> Future[T]:
    """
    Execute a callable in a background thread, returning a ``Future`` object
    that can be used to wait for the callable to exit (or check if it is
    still running.)

    This is similar to using ``concurrent.futures.ThreadPoolExecutor.submit``
    but does not require a thread pool. Since each invocation will create
    (and eventually shut down) new thread, this function is better suited for
    long-lived background tasks (and generally IO bound ones, at that) than
    distributing large workloads.
    """
    future: Future[T] = Future()

    def run() -> None:
        if not future.set_running_or_notify_cancel():
            return

        try:
            result = function()
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(result)

    Thread(target=run, name=name, daemon=daemon).start()

    return future


class Synchronized(Generic[T]):
    """
    This class wraps a value that is shared between multiple threads,
    providing thread-safe ``get`` and ``set`` methods for reading and writing
    (replacing) the value.
    """

    def __init__(self, value: T, lock: Optional[Lock] = None) -> None:
        if lock is None:
            lock = Lock()

        self.__value = value
        self.__lock = lock

    # TODO: For future use, it might make sense to expose the other lock
    # arguments on `get` and `set`, such as `timeout`, `block`, etc.

    @contextmanager
    def get(self) -> Iterator[T]:
        """
        Get the synchronized value.
        """
        with self.__lock:
            yield self.__value

    def set(self, value: T) -> None:
        """
        Set the synchronized value.
        """
        with self.__lock:
            self.__value = value
