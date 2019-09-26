import re

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
from snuba.query.query import QualifiedColumn
from snuba.query.timeseries import TimeSeriesExtension


class Groups(TimeSeriesDataset):
    """
    Experimental dataset that provides Groups data joined with
    the events table.
    """

    EVENTS_ALIAS = "events"
    GROUPS_ALIAS = "groups"

    QUALIFIED_COLUMN_REGEX = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_\.]+)$")

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
                    left=JoinConditionExpression(
                        table_alias=self.GROUPS_ALIAS,
                        column="project_id"),
                    right=JoinConditionExpression(
                        table_alias=self.EVENTS_ALIAS,
                        column="project_id"),
                ),
                JoinCondition(
                    left=JoinConditionExpression(
                        table_alias=self.GROUPS_ALIAS,
                        column="id"),
                    right=JoinConditionExpression(
                        table_alias=self.EVENTS_ALIAS,
                        column="group_id"),
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
            time_group_columns={
                'events.time': 'events.timestamp',
            },
        )

    def default_conditions(self):
        return [
            ('events.deleted', '=', 0),
            ('groups.record_deleted', '=', 0),
        ]

    def __parse_qualified_column(self, column_name: str) -> QualifiedColumn:
        # TODO: This logic should be general and in the Query class.
        # cannot do that yet since the column processing methods like
        # column_expr do not have access to the Query yet.
        match = self.QUALIFIED_COLUMN_REGEX.match(column_name)
        if not match or not match[1] in [self.EVENTS_ALIAS, self.GROUPS_ALIAS]:
            return QualifiedColumn(alias=None, column=column_name)
        else:
            return QualifiedColumn(alias=match[1], column=match[2])

    def column_expr(self, column_name, body):
        aliased_column = self.__parse_qualified_column(column_name)
        table_alias = aliased_column[0]
        if table_alias == self.GROUPS_ALIAS:
            return self.__grouped_message.column_expr(column_name, body)
        else:
            # this allows to delegate columns like tags[blabla] to
            # events without requiring them to be prefixed with the
            # table name.
            # That kind of logic that applies to the whole dataset should
            # be here, but this is not needed in this dataset since
            # only events have some custom column_expr logic.
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
