import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping

logger = logging.getLogger("snuba.writer")

WriterTableRow = Mapping[str, Any]


class BatchWriter(ABC):
    @abstractmethod
    def write(self, rows: Iterable[WriterTableRow]) -> None:
        raise NotImplementedError


class BufferedWriterWrapper:
    """
    This is a wrapper that adds a buffer around a BatchWriter.
    When consuming data from Kafka, the buffering logic is performed by the
    batching consumer.
    This is for the use cases that are not Kafka related.

    This is not thread safe. Don't try to do parallel flush hoping in the GIL.
    """

    def __init__(self, writer: BatchWriter, buffer_size: int):
        self.__writer = writer
        self.__buffer_size = buffer_size
        self.__buffer: List[WriterTableRow] = []

    def __flush(self) -> None:
        logger.debug("Flushing buffer with %d elements", len(self.__buffer))
        self.__writer.write(self.__buffer)
        self.__buffer = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.__buffer:
            self.__flush()

    def write(self, row: WriterTableRow):
        self.__buffer.append(row)
        if len(self.__buffer) >= self.__buffer_size:
            self.__flush()
