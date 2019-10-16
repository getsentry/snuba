import re

from datetime import timedelta
from typing import Mapping, Sequence, Union

from snuba.datasets.dataset import ColumnSplitSpec, TimeSeriesDataset
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinedSchema,
    JoinClause,
    JoinType,
    TableJoinNode,
)
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.query import Condition, Query


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
        groupedmessage_source = self.__grouped_message \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()

        self.__events = get_dataset("events")
        events_source = self.__events \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()

        join_structure = JoinClause(
            left_node=TableJoinNode(
                groupedmessage_source.format_from(),
                groupedmessage_source.get_columns(),
                self.GROUPS_ALIAS,
            ),
            right_node=TableJoinNode(
                events_source.format_from(),
                events_source.get_columns(),
                self.EVENTS_ALIAS,
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

    def default_conditions(self, table_alias: str="") -> Sequence[Condition]:
        events_conditions = self.__events.default_conditions(self.EVENTS_ALIAS)
        groups_conditions = self.__grouped_message.default_conditions(self.GROUPS_ALIAS)
        return events_conditions + groups_conditions

    def column_expr(self, column_name, query: Query, parsing_context: ParsingContext, table_alias: str=""):
        # Eventually joined dataset should not be represented by the same abstraction
        # as joinable datasets. That will be easier through the TableStorage abstraction.
        # Thus, as of now, receiving a table_alias here is not supported.
        assert table_alias == "", \
            "Groups dataset cannot be referenced with table aliases. Alias provided {table_alias}"

        match = self.QUALIFIED_COLUMN_REGEX.match(column_name)
        if not match:
            # anything that is not prefixed with a table alias is simply
            # escaped and returned. It could be a literal.
            return super().column_expr(column_name, query, parsing_context, table_alias)
        else:
            table_alias = match[1]
            simple_column_name = match[2]
            if table_alias == self.GROUPS_ALIAS:
                return self.__grouped_message.column_expr(simple_column_name, query, parsing_context, table_alias)
            elif table_alias == self.EVENTS_ALIAS:
                return self.__events.column_expr(simple_column_name, query, parsing_context, table_alias)
            else:
                # This is probably an error condition. To keep consistency with the behavior
                # in existing datasets, we let Clickhouse figure it out.
                return super().column_expr(simple_column_name, query, parsing_context, table_alias)

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

    def get_split_query_spec(self) -> Union[None, ColumnSplitSpec]:
        return ColumnSplitSpec(
            id_column="events.event_id",
            project_column="events.project_id",
            timestamp_column="events.timestamp",
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        # TODO: revisit how to build the prewhere clause on join
        # queries.
        return []
