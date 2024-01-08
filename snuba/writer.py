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


class BufferedWriterWrapper(Generic[TEncoded, TDecoded]):
    """
    This is a wrapper that adds a buffer around a BatchWriter.
    When consuming data from Kafka, the buffering logic is generally
    performed by the batch processor.
    This is for the use cases that are not Kafka related.

    This is not thread safe. Don't try to do parallel flush hoping in the GIL.
    """

    def __init__(
        self,
        writer: BatchWriter[TEncoded],
        buffer_size: int,
        encoder: Encoder[TEncoded, TDecoded],
    ):
        self.__writer = writer
        self.__buffer_size = buffer_size
        self.__buffer: List[TEncoded] = []
        self.__encoder = encoder

    def __flush(self) -> None:
        logger.debug("Flushing buffer with %d elements", len(self.__buffer))
        print(self.__buffer)
        self.__writer.write(self.__buffer)
        self.__buffer = []

    def __enter__(self) -> BufferedWriterWrapper[TEncoded, TDecoded]:
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        if self.__buffer:
            self.__flush()

    def write(self, row: TDecoded) -> None:
        self.__buffer.append(self.__encoder.encode(row))
        if len(self.__buffer) >= self.__buffer_size:
            self.__flush()
