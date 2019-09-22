from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinedSchema,
    JoinStructure,
    JoinType,
    SchemaJoinedSource,
)
from snuba.query.extensions import (
    PerformanceExtension,
    ProjectExtension,
    QueryExtension,
)
from snuba.query.timeseries import TimeSeriesExtension


class Groups(TimeSeriesDataset):
    """
    Experimental dataset that provides Groups data joined with
    the events table.
    """

    EVENTS_ALIAS = "events"
    GROUPS_ALIAS = "groups"

    def __init__(self) -> None:
        self.__grouped_message = get_dataset("groupedmessage")
        self.__events = get_dataset("events")

        join_structure = JoinStructure(
            left_source=SchemaJoinedSource(
                self.GROUPS_ALIAS,
                self.__grouped_message.get_dataset_schemas().get_read_schema(),
            ),
            right_source=SchemaJoinedSource(
                self.EVENTS_ALIAS,
                self.__events.get_dataset_schemas().get_read_schema(),
            ),
            mapping=[
                JoinCondition(
                    left=JoinConditionExpression(table_alias="groups", column="project_id"),
                    right=JoinConditionExpression(table_alias="events", column="project_id"),
                ),
                JoinCondition(
                    left=JoinConditionExpression(table_alias="groups", column="id"),
                    right=JoinConditionExpression(table_alias="events", column="group_id"),
                ),
            ],
            join_type=JoinType.LEFT,
        )

        schema = JoinedSchema(join_structure)
        dataset_schemas = DatasetSchemas(
            read_schema=schema,
            write_schema=None,
        )
        super().__init__(
            dataset_schemas=dataset_schemas,
            processor=None,
            default_topic=None,
            time_group_columns={
                'events.time': 'events.timestamp',
            },
        )

    def default_conditions(self):
        return [
            ('events.deleted', '=', 0),
            ('groups.record_deleted', '=', 0),
        ]

    def column_expr(self, column_name, body):
        # This dataset is meant to replace events and groupedmessage
        # (which will be TableStorage), thus column_expr will exist
        # only at this level.
        return self.__events.column_expr(column_name, body)

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            'performance': PerformanceExtension(),
            'project': ProjectExtension(),
            'timeseries': TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column='events.timestamp',
            ),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['events.event_id', 'events.project_id']
