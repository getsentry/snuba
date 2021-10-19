import logging
from typing import Callable, Iterable, Optional

from snuba.clickhouse.http import JSONRow
from snuba.clickhouse.native import ClickhousePool
from snuba.snapshots import BulkLoadSource, SnapshotTableRow
from snuba.snapshots.loaders import BulkLoader, ProgressCallback
from snuba.writer import BatchWriter, BufferedWriterWrapper, WriterTableRow

RowProcessor = Callable[[SnapshotTableRow], WriterTableRow]

logger = logging.getLogger("snuba.bulk-loader")


class SingleTableBulkLoader(BulkLoader):
    """
    Load data from a source table into one clickhouse destination table.
    """

    def __init__(
        self,
        source: BulkLoadSource,
        dest_table: str,
        source_table: str,
        row_processor: RowProcessor,
        clickhouse: ClickhousePool,
    ):
        self.__source = source
        self.__dest_table = dest_table
        self.__source_table = source_table
        self.__row_processor = row_processor
        self.__clickhouse = clickhouse

    def __validate_table(self, ignore_existing_data: bool) -> None:
        clickhouse_tables = self.__clickhouse.execute("show tables")
        if (self.__dest_table,) not in clickhouse_tables:
            raise ValueError("Destination table %s does not exists" % self.__dest_table)

        if not ignore_existing_data:
            table_content = self.__clickhouse.execute(
                "select count(*) from %s" % self.__dest_table
            )
            if table_content != [(0,)]:
                raise ValueError("Destination Table is not empty")

    def load(
        self,
        writer: BufferedWriterWrapper[JSONRow, WriterTableRow],
        ignore_existing_data: bool,
        progress_callback: Optional[ProgressCallback],
    ) -> None:
        self.__validate_table(ignore_existing_data)
        descriptor = self.__source.get_descriptor()
        logger.info("Loading snapshot %s", descriptor.id)

        with self.__source.get_parsed_table_file(self.__source_table) as table:
            logger.info("Loading table %s from file", self.__source_table)
            row_count = 0
            with writer as buffer_writer:
                for row in table:
                    clickhouse_data = self.__row_processor(row)
                    buffer_writer.write(clickhouse_data)
                    row_count += 1
            logger.info("Load complete %d records loaded", row_count)

    def load_preprocessed(
        self,
        writer: BatchWriter[bytes],
        ignore_existing_data: bool,
        progress_callback: Optional[ProgressCallback],
    ) -> None:
        self.__validate_table(ignore_existing_data)
        descriptor = self.__source.get_descriptor()
        logger.info("Loading snapshot %s", descriptor.id)

        def iterate_on_source(rows: Iterable[bytes]) -> Iterable[bytes]:
            size = 0
            for r in rows:
                if progress_callback is not None:
                    size += len(r)
                    progress_callback(size)
                yield r

        with self.__source.get_preprocessed_table_file(self.__source_table) as table:
            logger.info("Loading preprocessed table %s from file", self.__source_table)
            writer.write(iterate_on_source(table))
            logger.info("Load complete")
