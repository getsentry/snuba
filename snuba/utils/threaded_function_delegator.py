import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Callable, Generic, Iterator, List, Mapping, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor()


TInput = TypeVar("TInput")
TResult = TypeVar("TResult")


@dataclass(frozen=True)
class Result(Generic[TResult]):
    function_id: str
    result: TResult
    execution_time: float


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
        callback_func: Optional[Callable[[List[Result[TResult]]], None]],
    ) -> None:
        self.__callables = callables
        self.__selector_func = selector_func
        self.__callback_func = callback_func

    def __execute_callable(self, function_id: str) -> Result[TResult]:
        start_time = time.time()
        result = self.__callables[function_id]()
        end_time = time.time()
        execution_time = end_time - start_time
        return Result(function_id, result, execution_time)

    def __execute_callables(self, input: TInput) -> Iterator[Result[TResult]]:

        primary_function_id, secondary_function_ids = self.__selector_func(input)

        futures = [
            executor.submit(partial(self.__execute_callable, function_id=function_id))
            for function_id in secondary_function_ids
        ]

        yield self.__execute_callable(primary_function_id)

        yield from [future.result() for future in futures]

    def execute(self, input: TInput) -> TResult:
        generator = self.__execute_callables(input)

        results: List[Result[TResult]] = []

        try:
            result = next(generator)
            results.append(result)
            return result.result

        finally:

            def execute_callback() -> None:
                try:
                    for result in generator:
                        results.append(result)

                    if self.__callback_func is not None:
                        self.__callback_func(results)
                except Exception as error:
                    logger.exception(error)

            executor.submit(execute_callback)
