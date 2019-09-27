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
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.timeseries import TimeSeriesExtension


class Groups(TimeSeriesDataset):
    """
    Experimental dataset that provides Groups data joined with
    the events table.
    """

    EVENTS_ALIAS = "events"
    GROUPS_ALIAS = "groups"

    QUALIFIED_COLUMN_REGEX = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_\.\[\]]+)$")

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
            time_parse_columns=['events.timestamp'],
        )

    def default_conditions(self):
        return [
            ('events.deleted', '=', 0),
            ('groups.record_deleted', '=', 0),
        ]

    def column_expr(self, column_name, body, table_alias: str=""):
        # Eventually joined dataset should not be represented by the same abstraction
        # as joinable datasets. That will be easier through the TableStorage abstraciton.
        # Thus, as of now, receiving a table_alias here is not supported.
        assert table_alias == "", \
            "Groups dataset cannot be referenced with table aliases. Alias provided {table_alias}"

        match = self.QUALIFIED_COLUMN_REGEX.match(column_name)
        if not match:
            # anything that is not prefixed with a table alias is simply
            # escaped and returned. It could be a literal.
            return super().column_expr(column_name, body, table_alias)
        else:
            table_alias = match[1]
            simple_column_name = match[2]
            if table_alias == self.GROUPS_ALIAS:
                return self.__grouped_message.column_expr(simple_column_name, body, table_alias)
            elif table_alias == self.EVENTS_ALIAS:
                return self.__events.column_expr(simple_column_name, body, table_alias)
            else:
                # This is probably an error condition. To keep consistency with the behavior
                # in existing datasets, we let Clickhouse figure it out.
                return super().column_expr(simple_column_name, body, table_alias)

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            'project': ProjectExtension(
                processor=ProjectWithGroupsProcessor()
            ),
            'timeseries': TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column='events.timestamp',
            ),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['events.event_id', 'events.project_id']
