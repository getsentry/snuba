from snuba.clickhouse import ColumnSet, DateTime, Nullable, String, UInt
from snuba.datasets import DataSet
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageProcessor
from snuba.datasets.schema import ReplacingMergeTreeSchema


class GroupedMessageDataSet(DataSet):
    """
    This is a full clone of postgres groupedmessage
    generated through change capture.
    """

    def __init__(self):
        columns = ColumnSet([
            # columns to maintain the dataset
            # Kafka topic offset
            ('offset', UInt(64)),
            # PG columns
            ('id', UInt(64)),
            ('status', UInt(8)),
            ('last_seen', DateTime()),
            ('first_seen', DateTime()),
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

        super(GroupedMessageDataSet, self).__init__(
            schema=schema,
            processor=GroupedMessageProcessor(),
            default_topic="cdc",
            default_replacement_topic=None,
            default_commit_log_topic=None,
        )
