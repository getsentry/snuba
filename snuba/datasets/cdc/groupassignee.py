from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.cdc import CdcDataset
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
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
        return SingleTableBulkLoader(
            source=source,
            source_table=self.__postgres_table,
            dest_table=dest_table,
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

    def __init__(self) -> None:
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

        dataset_schemas = DatasetSchemas(read_schema=schema, write_schema=schema,)

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=GroupAssigneeTableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=GroupAssigneeProcessor(self.POSTGRES_TABLE),
                    default_topic="cdc",
                ),
                postgres_table=self.POSTGRES_TABLE,
            ),
            default_control_topic="cdc_control",
            postgres_table=self.POSTGRES_TABLE,
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "group_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            PrewhereProcessor(),
        ]
