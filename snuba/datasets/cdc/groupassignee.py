from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.clickhouse.pool import ClickhousePool
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.cdc import CdcDataset
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import SingleStorageSelector, WritableTableStorage
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.snapshots import BulkLoadSource
from snuba.snapshots.loaders.single_table import SingleTableBulkLoader


class GroupAssigneeTableWriter(TableWriter):
    def __init__(
        self, postgres_table: str, **kwargs,
    ):
        super().__init__(**kwargs)
        self.__postgres_table = postgres_table

    def get_bulk_loader(self, source: BulkLoadSource, dest_table: str):
        config = self.get_clickhouse_connection_config()
        clickhouse_client = ClickhousePool(config.host, config.port)

        return SingleTableBulkLoader(
            source=source,
            source_table=self.__postgres_table,
            dest_table=dest_table,
            clickhouse_client=clickhouse_client,
            row_processor=lambda row: GroupAssigneeRow.from_bulk(row).to_clickhouse(),
        )


class GroupAssigneeDataset(CdcDataset):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    POSTGRES_TABLE = "sentry_groupasignee"

    def __init__(self, clickhouse_connection_config: ClickhouseConnectionConfig) -> None:
        columns = ColumnSet(
            [
                # columns to maintain the dataset
                # Kafka topic offset
                ("offset", UInt(64)),
                ("record_deleted", UInt(8)),
                # PG columns
                ("project_id", UInt(64)),
                ("group_id", UInt(64)),
                ("date_added", Nullable(DateTime())),
                ("user_id", Nullable(UInt(64))),
                ("team_id", Nullable(UInt(64))),
            ]
        )

        schema = ReplacingMergeTreeSchema(
            columns=columns,
            local_table_name="groupassignee_local",
            dist_table_name="groupassignee_dist",
            order_by="(project_id, group_id)",
            partition_by=None,
            version_column="offset",
        )

        storage = WritableTableStorage(
            schemas=StorageSchemas(read_schema=schema, write_schema=schema),
            table_writer=GroupAssigneeTableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=GroupAssigneeProcessor(self.POSTGRES_TABLE),
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
        return ["project_id", "group_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
