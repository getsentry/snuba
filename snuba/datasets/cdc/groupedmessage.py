from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt

from snuba.datasets.cdc import CdcDataset
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageProcessor, GroupedMessageRow
from snuba.datasets.schema import ReplacingMergeTreeSchema
from snuba.snapshots.bulk_load import SingleTableBulkLoader


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
            order_by='id',
            partition_by=None,
            version_column='offset',
            sample_expr='id',
        )

        super(GroupedMessageDataset, self).__init__(
            write_schema=schema,
            processor=GroupedMessageProcessor(self.POSTGRES_TABLE),
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
            row_processor=lambda row: GroupedMessageRow.from_bulk(row).to_clickhouse(),
        )
