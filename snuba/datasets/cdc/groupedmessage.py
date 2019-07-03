from snuba.clickhouse import ColumnSet, DateTime, Nullable, UInt
from snuba.datasets import DataSet
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageProcessor
from snuba.datasets.schema import ReplacingMergeTreeSchema


class GroupedMessageDataSet(DataSet):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

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

        super(GroupedMessageDataSet, self).__init__(
            schema=schema,
            processor=GroupedMessageProcessor(),
            default_topic="cdc",
            default_replacement_topic=None,
            default_commit_log_topic=None,
        )
