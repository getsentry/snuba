from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping

import logging

from snuba.clickhouse import ClickhousePool
from snuba.snapshots import BulkLoadSource
from snuba.writer import BatchWriter, BufferedBatchWriter
from snuba import settings


class BulkLoader(ABC):
    """
    Loads data from a source into a Snuba dataset.

    Implementations can be dataset specific.
    The dataset returns an instance of this class to be used to perform
    the bulk load operation.
    """
    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError


class SingleTableBulkLoader(BulkLoader):
    """
    Load data from a source table into one clickhouse destination table.
    """

    def __init__(self,
                source: BulkLoadSource,
                dest_table: str,
                source_table: str,
                row_processor: Callable[[Mapping[str, Any]], Mapping[str, Any]],
            ):
        self.__source = source
        self.__dest_table = dest_table
        self.__source_table = source_table
        self.__row_processor = row_processor

    def load(self, writer: BatchWriter) -> None:
        logger = logging.getLogger('snuba.bulk-loader')

        clickhouse_ro = ClickhousePool(client_settings={
            'readonly': True,
        })
        clickhouse_tables = clickhouse_ro.execute('show tables')
        if (self.__dest_table,) not in clickhouse_tables:
            raise ValueError("Destination table %s does not exists" % self.__dest_table)

        table_content = clickhouse_ro.execute("select count(*) from %s" % self.__dest_table)
        if table_content != [(0,)]:
            raise ValueError("Destination Table is not empty")

        descriptor = self.__source.get_descriptor()
        logger.info("Loading snapshot %s", descriptor.id)

        with self.__source.get_table_file(self.__source_table) as table:
            logger.info("Loading table from file %s", self.__source_table)
            with BufferedBatchWriter(writer, settings.BULK_CLICKHOUSE_BUFFER) as buffer_writer:
                for row in table:
                    clickhouse_data = self.__row_processor(row)
                    buffer_writer.write(clickhouse_data)
