from datetime import timedelta
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.translators.snuba.mappers import ColumnToFunction
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors import promoted_tag_columns
from snuba.datasets.storages.factory import get_writable_storage
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension


errors_translators = TranslationMappers(
    columns=[
        ColumnToFunction(
            None, "user", "nullIf", (Column(None, None, "user"), Literal(None, ""))
        ),
    ]
)


class ErrorsEntity(Entity):
    """
    Represents the collections of all event types that are not transactions.

    This is meant to replace Events. They will both exist during the migration.
    """

    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.ERRORS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage, mappers=errors_translators
            ),
            abstract_column_set=columns,
            writable_storage=storage,
        )

    def _get_promoted_columns(self) -> Mapping[str, FrozenSet[str]]:
        return {
            "tags": frozenset(promoted_tag_columns.values()),
            "contexts": frozenset(),
        }

    def _get_column_tag_map(self) -> Mapping[str, Mapping[str, str]]:
        return {
            "tags": {col: tag for tag, col in promoted_tag_columns.items()},
            "contexts": {},
        }

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
            TimeSeriesProcessor(
                {"time": "timestamp", "rtime": "received"}, ("timestamp", "received")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            HandledFunctionsProcessor(
                "exception_stacks.mechanism_handled", self.get_data_model()
            ),
        ]
