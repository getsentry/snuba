from abc import ABC, abstractmethod

from snuba.clickhouse.native import ClickhousePool
from snuba.writer import BufferedWriterWrapper


class BulkLoader(ABC):
    """
    Loads data from a source into a Snuba dataset.

    Implementations can be dataset specific.
    The dataset returns an instance of this class to be used to perform
    the bulk load operation.
    """

    @abstractmethod
    def load(self, writer: BufferedWriterWrapper, clickhouse: ClickhousePool) -> None:
        # TODO: We shouldn't pass the clickhouse connection here, since both
        # writer and clickhouse are derived from the storage, this function
        # should probably just take a writable storage instead.
        raise NotImplementedError
