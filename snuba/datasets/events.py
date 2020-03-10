from datetime import timedelta
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_table import (
    SimpleQueryPlanExecutionStrategy,
    SingleTableQueryPlanBuilder,
)
from snuba.datasets.storages.events import (
    all_columns,
    metadata_columns,
    promoted_tag_columns,
    promoted_context_tag_columns,
    promoted_context_columns,
    required_columns,
    schema,
    storage,
)
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.sampling_rate import SamplingRateProcessor
from snuba.query.query import Query
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.util import qualified_column
from snuba.datasets.plans.split import (
    ColumnSplitSpec,
    SplitQueryPlanExecutionStrategy,
)


class EventsDataset(TimeSeriesDataset):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self) -> None:

        self.__metadata_columns = metadata_columns
        self.__promoted_tag_columns = promoted_tag_columns
        self.__promoted_context_tag_columns = promoted_context_tag_columns
        self.__promoted_context_columns = promoted_context_columns
        self.__required_columns = required_columns

        super(EventsDataset, self).__init__(
            storages=[storage],
            query_plan_builder=SingleTableQueryPlanBuilder(
                storage=storage,
                post_processors=[PrewhereProcessor(), SamplingRateProcessor()],
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
            promoted_columns=self._get_promoted_columns(),
            column_tag_map=self._get_column_tag_map(),
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

    def get_promoted_tag_columns(self) -> ColumnSet:
        return self.__promoted_tag_columns

    def _get_promoted_context_tag_columns(self) -> ColumnSet:
        return self.__promoted_context_tag_columns

    def _get_promoted_context_columns(self) -> ColumnSet:
        return self.__promoted_context_columns

    def get_required_columns(self) -> ColumnSet:
        return self.__required_columns

    def _get_promoted_columns(self) -> Mapping[str, FrozenSet[str]]:
        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        return {
            "tags": frozenset(
                col.flattened
                for col in (
                    self.get_promoted_tag_columns()
                    + self._get_promoted_context_tag_columns()
                )
            ),
            "contexts": frozenset(
                col.flattened for col in self._get_promoted_context_columns()
            ),
        }

    def _get_column_tag_map(self) -> Mapping[str, Mapping[str, str]]:
        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        promoted_context_tag_columns = self._get_promoted_context_tag_columns()

        return {
            "tags": {
                col.flattened: col.flattened.replace("_", ".")
                for col in promoted_context_tag_columns
            },
            "contexts": {},
        }

    def get_tag_column_map(self) -> Mapping[str, Mapping[str, str]]:
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items()))
            for col, trans in self._get_column_tag_map().items()
        }

    def get_promoted_tags(self) -> Mapping[str, Sequence[str]]:
        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        return {
            col: [
                self._get_column_tag_map()[col].get(x, x)
                for x in self._get_promoted_columns()[col]
            ]
            for col in self._get_promoted_columns()
        }

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
