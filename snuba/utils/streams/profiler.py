import logging
import time
from cProfile import Profile
from pathlib import Path
from typing import Callable, Mapping, Optional

from snuba.utils.streams.processing import ProcessingStrategy, ProcessingStrategyFactory
from snuba.utils.streams.types import Message, Partition, TPayload


logger = logging.getLogger(__name__)


class ProcessingStrategyProfilerWrapper(ProcessingStrategy[TPayload]):
    def __init__(
        self,
        strategy: ProcessingStrategy[TPayload],
        profiler: Profile,
        output_path: str,
    ) -> None:
        self.__strategy = strategy
        self.__profiler = profiler
        self.__output_path = output_path

    def poll(self) -> None:
        self.__strategy.poll()

    def submit(self, message: Message[TPayload]) -> None:
        self.__strategy.submit(message)

    def close(self) -> None:
        self.__strategy.close()

    def terminate(self) -> None:
        self.__strategy.terminate()
        self.__profiler.disable()  # not sure if necessary
        logger.info(
            "Writing profile data from %r to %r...", self.__profiler, self.__output_path
        )
        self.__profiler.dump_stats(self.__output_path)

    def join(self, timeout: Optional[float] = None) -> None:
        self.__strategy.join(timeout)
        self.__profiler.disable()  # not sure if necessary
        logger.info(
            "Writing profile data from %r to %r...", self.__profiler, self.__output_path
        )
        self.__profiler.dump_stats(self.__output_path)


class ProcessingStrategyProfilerWrapperFactory(ProcessingStrategyFactory[TPayload]):
    def __init__(
        self,
        strategy_factory: ProcessingStrategyFactory[TPayload],
        output_directory: str,
    ) -> None:
        self.__strategy_factory = strategy_factory
        self.__output_directory = Path(output_directory)
        assert self.__output_directory.exists() and self.__output_directory.is_dir()

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[TPayload]:
        profiler = Profile()
        profiler.enable()
        return ProcessingStrategyProfilerWrapper(
            self.__strategy_factory.create(commit),
            profiler,
            str(self.__output_directory / f"{int(time.time() * 1000)}.prof"),
        )
