from datetime import timedelta
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

from snuba.clickhouse.processors import QueryProcessor as ClickhouseProcessor
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.entity import Entity
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas import MandatoryCondition
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
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor as LogicalProcessor
from snuba.query.processors.join_optimizers import SimpleJoinOptimizer
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.util import parse_datetime, qualified_column


class JoinedStorage(ReadableStorage):
    def __init__(
        self, storage_set_key: StorageSetKey, join_structure: JoinClause,
    ) -> None:
        self.__structure = join_structure
        super().__init__(storage_set_key, JoinedSchema(self.__structure))

    def get_table_writer(self) -> Optional[TableWriter]:
        return None

    def get_query_processors(self) -> Sequence[ClickhouseProcessor]:
        return [SimpleJoinOptimizer(), PrewhereProcessor()]


class GroupsEntity(Entity):
    """
    Experimental entity that provides Groups data joined with
    the events table.
    """

    EVENTS_ALIAS = "events"
    GROUPS_ALIAS = "groups"

    def __init__(self) -> None:
        self.__grouped_message = get_entity(EntityKey.GROUPEDMESSAGES)
        groupedmessage_source = (
            get_storage(StorageKey.GROUPEDMESSAGES).get_schema().get_data_source()
        )

        self.__events = get_entity(EntityKey.EVENTS)
        events_source = get_storage(StorageKey.EVENTS).get_schema().get_data_source()

        join_structure = JoinClause(
            left_node=TableJoinNode(
                table_name=groupedmessage_source.format_from(),
                columns=groupedmessage_source.get_columns(),
                mandatory_conditions=[
                    MandatoryCondition(
                        (qualified_column("record_deleted", self.GROUPS_ALIAS), "=", 0),
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            Column(None, self.GROUPS_ALIAS, "record_deleted"),
                            Literal(None, 0),
                        ),
                    )
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
                    MandatoryCondition(
                        (qualified_column("deleted", self.EVENTS_ALIAS), "=", 0),
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            Column(None, self.EVENTS_ALIAS, "deleted"),
                            Literal(None, 0),
                        ),
                    )
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
        storage = JoinedStorage(StorageSetKey.EVENTS, join_structure)
        self.__time_group_columns = {"events.time": "events.timestamp"}
        self.__time_parse_columns = [
            "events.timestamp",
            "events.received",
            "groups.last_seen",
            "groups.first_seen",
            "groups.active_at",
        ]
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=schema.get_columns(),
            writable_storage=None,
        )

    def column_expr(
        self,
        column_name: str,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> Union[None, Any]:
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
            "project": ProjectExtension(project_column="events.project_id"),
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

    # TODO: This needs to burned with fire, for so many reasons.
    # It's here now to reduce the scope of the initial entity changes
    # but can be moved to a processor if not removed entirely.
    def process_condition(
        self, condition: Tuple[str, str, Any]
    ) -> Tuple[str, str, Any]:
        lhs, op, lit = condition
        if (
            lhs in self.__time_parse_columns
            and op in (">", "<", ">=", "<=", "=", "!=")
            and isinstance(lit, str)
        ):
            lit = parse_datetime(lit)
        return lhs, op, lit
