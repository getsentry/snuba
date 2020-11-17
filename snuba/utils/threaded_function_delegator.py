import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Generic, Iterator, List, Mapping, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor()


TInput = TypeVar("TInput")
TResult = TypeVar("TResult")


class ThreadedFunctionDelegator(Generic[TInput, TResult]):
    """
    Executes one or more functions against the input provided.

    The list of callables that are executed are determined by the selector function.
    The first item returned by the selector_func determines the primary result that
    will be returned from "execute". Subsequent callables that are also selected
    are run in separate threads. The results of all functions are available via the
    callback function if one is provided.
    """

    def __init__(
        self,
        callables: Mapping[str, Callable[[], TResult]],
        selector_func: Callable[[TInput], Tuple[str, List[str]]],
        callback_func: Optional[Callable[[List[Tuple[str, TResult]]], None]],
    ) -> None:
        self.__callables = callables
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def __execute_callables(self, input: TInput) -> Iterator[Tuple[str, TResult]]:
        selected = self.__selector_func(input)

        primary_function_id = selected[0]

        futures = [
            (function_id, executor.submit(self.__callables[function_id]))
            for function_id in selected[1]
        ]

        yield primary_function_id, self.__callables[primary_function_id]()

        yield from [(function_id, future.result()) for (function_id, future) in futures]

    def execute(self, input: TInput) -> TResult:
        generator = self.__execute_callables(input)

        results: List[Tuple[str, TResult]] = []

        try:
            result_id, result = next(generator)
            results.append((result_id, result))
            return result

        finally:

            def execute_callback() -> None:
                try:
                    for result_id, result in generator:
                        results.append((result_id, result))

                    if self.__callback_func is not None:
                        self.__callback_func(results)
                except Exception as error:
                    logger.exception(error)

            executor.submit(execute_callback)
