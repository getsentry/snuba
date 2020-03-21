from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.clickhouse.pool import ClickhousePool

from snuba.datasets.cdc import CdcDataset
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.cdc.groupedmessage_processor import (
    GroupedMessageProcessor,
    GroupedMessageRow,
)
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import SingleStorageSelector, WritableTableStorage
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.snapshots.loaders.single_table import SingleTableBulkLoader


class GroupedMessageTableWriter(TableWriter):
    def __init__(
        self, postgres_table: str, **kwargs,
    ):
        super().__init__(**kwargs)
        self.__postgres_table = postgres_table

    def get_bulk_loader(self, source, dest_table):
        config = self.get_clickhouse_connection_config()
        clickhouse_client = ClickhousePool(config.host, config.port)
        return SingleTableBulkLoader(
            source=source,
            source_table=self.__postgres_table,
            dest_table=dest_table,
            clickhouse_client=clickhouse_client,
            row_processor=lambda row: GroupedMessageRow.from_bulk(row).to_clickhouse(),
        )


class GroupedMessageDataset(CdcDataset):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

    POSTGRES_TABLE = "sentry_groupedmessage"

    def __init__(self, clickhouse_connection_config: ClickhouseConnectionConfig) -> None:
        columns = ColumnSet(
            [
                # columns to maintain the dataset
                # Kafka topic offset
                ("offset", UInt(64)),
                # GroupStatus in Sentry does not have a 'DELETED' state that reflects the deletion
                # of the record. Having a dedicated clickhouse-only flag to identify this case seems
                # more consistent than add an additional value into the status field below that does not
                # exists on the Sentry side.
                ("record_deleted", UInt(8)),
                # PG columns
                ("project_id", UInt(64)),
                ("id", UInt(64)),
                ("status", Nullable(UInt(8))),
                ("last_seen", Nullable(DateTime())),
                ("first_seen", Nullable(DateTime())),
                ("active_at", Nullable(DateTime())),
                ("first_release_id", Nullable(UInt(64))),
            ]
        )

        schema = ReplacingMergeTreeSchema(
            columns=columns,
            local_table_name="groupedmessage_local",
            dist_table_name="groupedmessage_dist",
            mandatory_conditions=[("record_deleted", "=", 0)],
            prewhere_candidates=["project_id", "id"],
            order_by="(project_id, id)",
            partition_by=None,
            version_column="offset",
            sample_expr="id",
        )

        storage = WritableTableStorage(
            schemas=StorageSchemas(read_schema=schema, write_schema=schema),
            table_writer=GroupedMessageTableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=GroupedMessageProcessor(self.POSTGRES_TABLE),
                    default_topic="cdc",
                ),
                clickhouse_connection_config=clickhouse_connection_config,
                postgres_table=self.POSTGRES_TABLE,
            ),
            query_processors=[PrewhereProcessor()],
        )

        storage_selector = SingleStorageSelector(storage=storage)

        super().__init__(
            storages=[storage],
            storage_selector=storage_selector,
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            default_control_topic="cdc_control",
            postgres_table=self.POSTGRES_TABLE,
            clickhouse_connection_config=clickhouse_connection_config,
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
