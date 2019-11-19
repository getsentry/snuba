from typing import Callable

import logging

from snuba.clickhouse.native import ClickhousePool
from snuba.snapshots import BulkLoadSource
from snuba.writer import BufferedWriterWrapper, WriterTableRow
from snuba.snapshots import SnapshotTableRow
from snuba.snapshots.loaders import BulkLoader


class SingleTableBulkLoader(BulkLoader):
    """
    Load data from a source table into one clickhouse destination table.
    """

    def __init__(
        self,
        source: BulkLoadSource,
        dest_table: str,
        source_table: str,
        row_processor: Callable[[SnapshotTableRow], WriterTableRow],
    ):
        self.__source = source
        self.__dest_table = dest_table
        self.__source_table = source_table
        self.__row_processor = row_processor

    def load(self, writer: BufferedWriterWrapper) -> None:
        logger = logging.getLogger("snuba.bulk-loader")

        clickhouse_ro = ClickhousePool(client_settings={"readonly": True})
        clickhouse_tables = clickhouse_ro.execute("show tables")
        if (self.__dest_table,) not in clickhouse_tables:
            raise ValueError("Destination table %s does not exists" % self.__dest_table)

        table_content = clickhouse_ro.execute(
            "select count(*) from %s" % self.__dest_table
        )
        if table_content != [(0,)]:
            raise ValueError("Destination Table is not empty")

        descriptor = self.__source.get_descriptor()
        logger.info("Loading snapshot %s", descriptor.id)

        with self.__source.get_table_file(self.__source_table) as table:
            logger.info("Loading table %s from file", self.__source_table)
            row_count = 0
            with writer as buffer_writer:
                for row in table:
                    clickhouse_data = self.__row_processor(row)
                    buffer_writer.write(clickhouse_data)
                    row_count += 1
            logger.info("Load complete %d records loaded", row_count)
