from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt

from snuba.datasets.cdc import CdcDataset
from snuba.datasets.cdc.cdcprocessors import CdcProcessor
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageProcessor, GroupedMessageRow
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.table_storage import KafkaFedTableWriter
from snuba.snapshots.bulk_load import SingleTableBulkLoader


class GroupedMessageTableWriter(KafkaFedTableWriter):
    def __init__(self,
                postgres_table: str,
                processor: CdcProcessor,
                **kwargs,
            ):
        super().__init__(
            processor=processor,
            **kwargs
        )
        self.__postgres_table = postgres_table

    def get_bulk_loader(self, source, dest_table):
        return SingleTableBulkLoader(
            source=source,
            source_table=self.__postgres_table,
            dest_table=dest_table,
            row_processor=lambda row: GroupedMessageRow.from_bulk(row).to_clickhouse(),
        )


class GroupedMessageDataset(CdcDataset):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

    POSTGRES_TABLE = 'sentry_groupedmessage'

    def __init__(self):
        columns = ColumnSet([
            # columns to maintain the dataset
            # Kafka topic offset
            ('offset', UInt(64)),
            # GroupStatus in Sentry does not have a 'DELETED' state that reflects the deletion
            # of the record. Having a dedicated clickhouse-only flag to identify this case seems
            # more consistent than add an additional value into the status field below that does not
            # exists on the Sentry side.
            ('record_deleted', UInt(8)),
            # PG columns
            ('project_id', UInt(64)),
            ('id', UInt(64)),
            ('status', Nullable(UInt(8))),
            ('last_seen', Nullable(DateTime())),
            ('first_seen', Nullable(DateTime())),
            ('active_at', Nullable(DateTime())),
            ('first_release_id', Nullable(UInt(64))),
        ])

        schema = ReplacingMergeTreeSchema(
            columns=columns,
            local_table_name='groupedmessage_local',
            dist_table_name='groupedmessage_dist',
            order_by='(project_id, id)',
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
            table_writer=GroupedMessageTableWriter(
                postgres_table=self.POSTGRES_TABLE,
                processor=GroupedMessageProcessor(self.POSTGRES_TABLE),
                write_schema=schema,
                default_topic="cdc",
            ),
            default_control_topic="cdc_control",
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['project_id']
