from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.processors.prewhere import PrewhereProcessor
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

POSTGRES_TABLE = "sentry_groupasignee"

storage = CdcStorage(
    schemas=StorageSchemas(read_schema=schema, write_schema=schema),
    table_writer=GroupAssigneeTableWriter(
        write_schema=schema,
        stream_loader=KafkaStreamLoader(
            processor=GroupAssigneeProcessor(POSTGRES_TABLE),
            default_topic="cdc",
            pre_filter=CdcTableNameMessageFilter(POSTGRES_TABLE),
        ),
        postgres_table=POSTGRES_TABLE,
    ),
    query_processors=[PrewhereProcessor()],
    default_control_topic="cdc_control",
    postgres_table=POSTGRES_TABLE,
)
