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
            ('logger', String()),
            ('level', UInt(32)),
            ('message', String()),
            ('view', Nullable(String())),
            ('status', UInt(32)),
            #('times_seen', UInt(64)),
            ('last_seen', DateTime()),
            ('first_seen', DateTime()),
            #('data', Nullable(String())),
            #('score', UInt(32)),
            ('project_id', UInt(64)),
            #('time_spent_total', UInt(32)),
            #('time_spent_count', UInt(32)),
            #('resolved_at', Nullable(DateTime())),
            ('active_at', Nullable(DateTime())),
            #('is_public', Nullable(UInt(8))),
            ('platform', Nullable(String())),
            #('num_comments', Nullable(UInt(32))),
            ('first_release_id', Nullable(UInt(64))),
            #('short_id', Nullable(UInt(64))),
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
