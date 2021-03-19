from abc import ABC, abstractmethod
from typing import Callable, Optional

from snuba.clickhouse.http import JSONRow
from snuba.writer import BatchWriter, BufferedWriterWrapper, WriterTableRow

ProgressCallback = Callable[[int], None]


class BulkLoader(ABC):
    """
    Loads data from a source into a Snuba dataset.

    Implementations can be dataset specific.
    The dataset returns an instance of this class to be used to perform
    the bulk load operation.
    """

    @abstractmethod
    def load(
        self,
        writer: BufferedWriterWrapper[JSONRow, WriterTableRow],
        ignore_existing_data: bool,
        progress_callback: Optional[ProgressCallback],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_preprocessed(
        self,
        writer: BatchWriter[bytes],
        ignore_existing_data: bool,
        progress_callback: Optional[ProgressCallback],
    ) -> None:
        raise NotImplementedError
