from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.cdc import CdcDataset
from snuba.datasets.cdc.groupassignee_processor import GroupAssigneeProcessor, GroupAssigneeRow
from snuba.datasets.schema import ReplacingMergeTreeSchema
from snuba.snapshots.bulk_load import SingleTableBulkLoader


class GroupAssigneeDataset(CdcDataset):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    POSTGRES_TABLE = "sentry_groupasignee"

    def __init__(self) -> None:
        columns = ColumnSet([
            # columns to maintain the dataset
            # Kafka topic offset
            ("offset", UInt(64)),
            ("record_deleted", UInt(8)),
            # PG columns
            ("id", UInt(64)),
            ("project_id", UInt(64)),
            ("group_id", UInt(64)),
            ("date_added", Nullable(DateTime())),
            ("user_id", Nullable(UInt(64))),
            ("team_id", Nullable(UInt(64))),
        ])

        schema = ReplacingMergeTreeSchema(
            columns=columns,
            local_table_name='groupassignee_local',
            dist_table_name='groupassignee_dist',
            # TODO: add project id and group id to the identity key in postgres
            # and then add them here.
            order_by='(id)',
            partition_by=None,
            version_column='offset',
            sample_expr='id',
        )

        dataset_schemas = DatasetSchemas(
            read_schema=schema,
            write_schema=schema,
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            processor=GroupAssigneeProcessor(self.POSTGRES_TABLE),
            default_topic="cdc",
            default_replacement_topic=None,
            default_commit_log_topic=None,
            default_control_topic="cdc_control",
        )

    def get_bulk_loader(self, source, dest_table):
        return SingleTableBulkLoader(
            source=source,
            source_table=self.POSTGRES_TABLE,
            dest_table=dest_table,
            row_processor=lambda row: GroupAssigneeRow.from_bulk(row).to_clickhouse(),
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "group_id"]
