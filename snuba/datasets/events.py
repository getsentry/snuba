from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.events import get_column_tag_map, get_promoted_columns
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.util import qualified_column


class EventsDataset(TimeSeriesDataset):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.EVENTS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()
        self.__time_group_columns = {"time": "timestamp", "rtime": "received"}
        super(EventsDataset, self).__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=columns,
            writable_storage=storage,
            time_group_columns=self.__time_group_columns,
            time_parse_columns=("timestamp", "received"),
        )

        self.__tags_processor = TagColumnProcessor(
            columns=columns,
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

            # This conversion must not be ported to the errors dataset. We should
            # not support promoting tags/contexts with boolean values. There is
            # no way to convert them back consistently to the value provided by
            # the client when the event is ingested, in all ways to access
            # tags/contexts. Once the errors dataset is in use, we will not have
            # boolean promoted tags/contexts so this constraint will be easy to enforce.
            boolean_contexts = {
                "contexts[device.simulator]",
                "contexts[device.online]",
                "contexts[device.charging]",
            }
            boolean_context_template = (
                "multiIf(%(processed_column)s == '', '', "
                "%(processed_column)s IN ('1', 'True'), 'True', 'False')"
            )
            if column_name in boolean_contexts:
                return boolean_context_template % (
                    {"processed_column": processed_column}
                )
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
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]
