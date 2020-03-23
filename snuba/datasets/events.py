from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import (
    SimpleQueryPlanExecutionStrategy,
    SingleStorageQueryPlanBuilder,
)
from snuba.datasets.plans.split import (
    ColumnSplitSpec,
    SplitQueryPlanExecutionStrategy,
)
from snuba.datasets.storages.events import (
    all_columns,
    get_column_tag_map,
    get_promoted_columns,
    schema,
    storage,
)
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.util import qualified_column


class EventsDataset(TimeSeriesDataset):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self) -> None:

        super(EventsDataset, self).__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage,
                execution_strategy=SplitQueryPlanExecutionStrategy(
                    ColumnSplitSpec(
                        id_column="event_id",
                        project_column="project_id",
                        timestamp_column="timestamp",
                    ),
                    default_strategy=SimpleQueryPlanExecutionStrategy(),
                ),
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            time_group_columns={"time": "timestamp", "rtime": "received"},
            time_parse_columns=("timestamp", "received"),
        )

        self.__tags_processor = TagColumnProcessor(
            columns=all_columns,
            promoted_columns=get_promoted_columns(),
            column_tag_map=get_column_tag_map(),
        )

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        processed_column = self.__tags_processor.process_column_expression(
            column_name, query, parsing_context, table_alias
        )
        if processed_column:
            # If processed_column is None, this was not a tag/context expression
            return processed_column
        elif column_name == "group_id":
            return f"nullIf({qualified_column('group_id', table_alias)}, 0)"
        elif column_name == "message":
            # Because of the rename from message->search_message without backfill,
            # records will have one or the other of these fields.
            # TODO this can be removed once all data has search_message filled in.
            search_message = qualified_column("search_message", table_alias)
            message = qualified_column("message", table_alias)
            return f"coalesce({search_message}, {message})"
        else:
            return super().column_expr(column_name, query, parsing_context, table_alias)

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(
                    project_column="project_id",
                    # key migration is on going. As soon as all the keys we are interested
                    # into in redis are stored with "EVENTS" in the name, we can change this.
                    replacer_state_name=None,
                )
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
