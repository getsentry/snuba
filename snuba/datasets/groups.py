from datetime import timedelta
from typing import Mapping, Optional, Sequence

from snuba.clickhouse.processors import QueryProcessor as ClickhouseProcessor
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.schemas.resolver import JoinedTablesResolver
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.join import (
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinedSchema,
    JoinType,
    TableJoinNode,
)
from snuba.datasets.storage import ReadableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.table_storage import TableWriter
from snuba.query.columns import QUALIFIED_COLUMN_REGEX
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor as LogicalProcessor
from snuba.query.processors.join_optimizers import SimpleJoinOptimizer
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.util import qualified_column


class JoinedStorage(ReadableStorage):
    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        join_structure: JoinClause,
    ) -> None:
        self.__structure = join_structure
        super().__init__(storage_key, storage_set_key)

    def get_schemas(self) -> StorageSchemas:
        return StorageSchemas(
            read_schema=JoinedSchema(self.__structure), write_schema=None
        )

    def get_table_writer(self) -> Optional[TableWriter]:
        return None

    def get_query_processors(self) -> Sequence[ClickhouseProcessor]:
        return [SimpleJoinOptimizer(), PrewhereProcessor()]


class Groups(TimeSeriesDataset):
    """
    Experimental dataset that provides Groups data joined with
    the events table.
    """

    EVENTS_ALIAS = "events"
    GROUPS_ALIAS = "groups"

    def __init__(self) -> None:
        self.__grouped_message = get_dataset("groupedmessage")
        groupedmessage_source = (
            get_storage(StorageKey.GROUPEDMESSAGES)
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )

        self.__events = get_dataset("events")
        events_source = (
            get_storage(StorageKey.EVENTS)
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )

        join_structure = JoinClause(
            left_node=TableJoinNode(
                table_name=groupedmessage_source.format_from(),
                columns=groupedmessage_source.get_columns(),
                mandatory_conditions=[
                    # TODO: This will be replaced as soon as expressions won't be strings
                    # thus we will be able to easily add an alias to a column in an
                    # expression.
                    (qualified_column("record_deleted", self.GROUPS_ALIAS), "=", 0)
                ],
                prewhere_candidates=[
                    qualified_column(col, self.GROUPS_ALIAS)
                    for col in groupedmessage_source.get_prewhere_candidates()
                ],
                alias=self.GROUPS_ALIAS,
            ),
            right_node=TableJoinNode(
                table_name=events_source.format_from(),
                columns=events_source.get_columns(),
                mandatory_conditions=[
                    (qualified_column("deleted", self.EVENTS_ALIAS), "=", 0)
                ],
                prewhere_candidates=[
                    qualified_column(col, self.EVENTS_ALIAS)
                    for col in events_source.get_prewhere_candidates()
                ],
                alias=self.EVENTS_ALIAS,
            ),
            mapping=[
                JoinCondition(
                    left=JoinConditionExpression(
                        table_alias=self.GROUPS_ALIAS, column="project_id"
                    ),
                    right=JoinConditionExpression(
                        table_alias=self.EVENTS_ALIAS, column="project_id"
                    ),
                ),
                JoinCondition(
                    left=JoinConditionExpression(
                        table_alias=self.GROUPS_ALIAS, column="id"
                    ),
                    right=JoinConditionExpression(
                        table_alias=self.EVENTS_ALIAS, column="group_id"
                    ),
                ),
            ],
            join_type=JoinType.LEFT,
        )

        schema = JoinedSchema(join_structure)
        storage = JoinedStorage(StorageKey.GROUPS, StorageSetKey.EVENTS, join_structure)
        self.__time_group_columns = {"events.time": "events.timestamp"}
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=schema.get_columns(),
            writable_storage=None,
            column_resolver=JoinedTablesResolver(
                join_structure, {self.EVENTS_ALIAS: ["tags_key", "tags_value"],}
            ),
            time_group_columns=self.__time_group_columns,
            time_parse_columns=[
                "events.timestamp",
                "events.received",
                "groups.last_seen",
                "groups.first_seen",
                "groups.active_at",
            ],
        )

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        # Eventually joined dataset should not be represented by the same abstraction
        # as joinable datasets. That will be easier through the TableStorage abstraction.
        # Thus, as of now, receiving a table_alias here is not supported.
        assert (
            table_alias == ""
        ), "Groups dataset cannot be referenced with table aliases. Alias provided {table_alias}"

        match = QUALIFIED_COLUMN_REGEX.match(column_name)
        if not match:
            # anything that is not prefixed with a table alias is simply
            # escaped and returned. It could be a literal.
            return super().column_expr(column_name, query, parsing_context, table_alias)
        else:
            table_alias = match[1]
            simple_column_name = match[2]
            if table_alias == self.GROUPS_ALIAS:
                return self.__grouped_message.column_expr(
                    simple_column_name, query, parsing_context, table_alias
                )
            elif table_alias == self.EVENTS_ALIAS:
                return self.__events.column_expr(
                    simple_column_name, query, parsing_context, table_alias
                )
            else:
                # This is probably an error condition. To keep consistency with the behavior
                # in existing datasets, we let Clickhouse figure it out.
                return super().column_expr(
                    simple_column_name, query, parsing_context, table_alias
                )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(
                    project_column="events.project_id", replacer_state_name=None,
                )
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="events.timestamp",
            ),
        }

    def get_query_processors(self) -> Sequence[LogicalProcessor]:
        return [
            TagsExpanderProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]
