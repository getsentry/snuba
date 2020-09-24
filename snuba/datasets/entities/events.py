from datetime import timedelta
from typing import Any, Mapping, Sequence, Tuple

from snuba import state
from snuba.clickhouse.translators.snuba.mappers import SubscriptableMapper
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import (
    QueryStorageSelector,
    ReadableStorage,
    StorageAndMappers,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.events_common import (
    get_column_tag_map,
    get_promoted_columns,
)
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.util import parse_datetime

# TODO: This will be a property of the relationship between entity and
# storage. Now we do not have entities so it is between dataset and
# storage.
event_translator = TranslationMappers(
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
    ],
)


class EventsQueryStorageSelector(QueryStorageSelector):
    def __init__(
        self, events_table: ReadableStorage, events_ro_table: ReadableStorage,
    ) -> None:
        self.__events_table = events_table
        self.__events_ro_table = events_ro_table

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not request_settings.get_consistent()
        )

        storage = (
            self.__events_ro_table if use_readonly_storage else self.__events_table
        )
        return StorageAndMappers(storage, event_translator)


class EventsEntity(Entity):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.EVENTS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()
        ro_storage = get_storage(StorageKey.EVENTS_RO)

        self.__time_group_columns = {"time": "timestamp", "rtime": "received"}
        self.__time_parse_columns = ("timestamp", "received")
        super().__init__(
            storages=[storage],
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=EventsQueryStorageSelector(
                    events_table=storage, events_ro_table=ro_storage,
                )
            ),
            abstract_column_set=columns,
            writable_storage=storage,
        )

        self.__tags_processor = TagColumnProcessor(
            columns=columns,
            promoted_columns=get_promoted_columns(),
            column_tag_map=get_column_tag_map(),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
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
            HandledFunctionsProcessor(
                "exception_stacks.mechanism_handled", self.get_data_model()
            ),
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
