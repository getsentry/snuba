from typing import Any, Optional, MutableMapping, Sequence, Type

from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc.cdcprocessors import CdcMessageRow
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader, TableWriter
from snuba.query.processors import QueryProcessor
from snuba.replacers.replacer_processor import ReplacerProcessor
from snuba.snapshots.loaders.single_table import SingleTableBulkLoader


class CdcTableWriter(TableWriter):
    def __init__(
        self, postgres_table: str, message_row: Type[CdcMessageRow], **kwargs,
    ):
        super().__init__(**kwargs)
        self.__postgres_table = postgres_table
        self.__message_row = message_row

    def get_bulk_loader(self, source, dest_table) -> SingleTableBulkLoader:
        return SingleTableBulkLoader(
            source=source,
            source_table=self.__postgres_table,
            dest_table=dest_table,
            row_processor=lambda row: self.__message_row.from_bulk(row).to_clickhouse(),
        )


class CdcStorage(WritableTableStorage):
    def __init__(
        self,
        *,
        default_control_topic: str,
        postgres_table: str,
        message_row: Type[CdcMessageRow],
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        schemas: StorageSchemas,
        query_processors: Sequence[QueryProcessor],
        stream_loader: KafkaStreamLoader,
        replacer_processor: Optional[ReplacerProcessor] = None,
        writer_options: Optional[MutableMapping[str, Any]] = None,
    ):
        super().__init__(
            storage_key=storage_key,
            storage_set_key=storage_set_key,
            schemas=schemas,
            query_processors=query_processors,
            stream_loader=stream_loader,
            replacer_processor=replacer_processor,
            writer_options=writer_options,
        )
        self.__default_control_topic = default_control_topic
        self.__postgres_table = postgres_table

        write_schema = schemas.get_write_schema()
        assert write_schema is not None
        self.__table_writer = CdcTableWriter(
            postgres_table=postgres_table,
            message_row=message_row,
            cluster=get_cluster(storage_set_key),
            write_schema=write_schema,
            stream_loader=stream_loader,
            replacer_processor=replacer_processor,
            writer_options=writer_options,
        )

    def get_table_writer(self) -> TableWriter:
        return self.__table_writer

    def get_default_control_topic(self) -> str:
        return self.__default_control_topic

    def get_postgres_table(self) -> str:
        return self.__postgres_table
