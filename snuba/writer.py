from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, Iterable, List, Mapping, TypeVar

from snuba.utils.codecs import Encoder, TDecoded, TEncoded

logger = logging.getLogger("snuba.writer")

WriterTableRow = Mapping[str, Any]


T = TypeVar("T")


class BatchWriter(ABC, Generic[T]):
    @abstractmethod
    def write(self, values: Iterable[T]) -> None:
        raise NotImplementedError


class BatchWriterEncoderWrapper(BatchWriter[TDecoded]):
    def __init__(
        self, writer: BatchWriter[TEncoded], encoder: Encoder[TEncoded, TDecoded]
    ) -> None:
        self.__writer = writer
        self.__encoder = encoder

    def write(self, values: Iterable[TDecoded]) -> None:
        return self.__writer.write(map(self.__encoder.encode, values))


class BufferedWriterWrapper:
    """
    This is a wrapper that adds a buffer around a BatchWriter.
    When consuming data from Kafka, the buffering logic is performed by the
    batching consumer.
    This is for the use cases that are not Kafka related.

    This is not thread safe. Don't try to do parallel flush hoping in the GIL.
    """

    def __init__(self, writer: BatchWriter[WriterTableRow], buffer_size: int):
        self.__writer = writer
        self.__buffer_size = buffer_size
        self.__buffer: List[WriterTableRow] = []

    def __flush(self) -> None:
        logger.debug("Flushing buffer with %d elements", len(self.__buffer))
        self.__writer.write(self.__buffer)
        self.__buffer = []

    def __enter__(self) -> BufferedWriterWrapper:
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        if self.__buffer:
            self.__flush()

    def write(self, row: WriterTableRow) -> None:
        self.__buffer.append(row)
        if len(self.__buffer) >= self.__buffer_size:
            self.__flush()
