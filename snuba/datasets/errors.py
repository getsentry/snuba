from datetime import timedelta
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.translators.snuba.mappers import ColumnToFunction
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors import promoted_tag_columns
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension


errors_translators = TranslationMappers(
    columns=[
        ColumnToFunction(
            None, "user", "nullIf", (Column(None, None, "user"), Literal(None, ""))
        ),
    ]
)


class ErrorsDataset(TimeSeriesDataset):
    """
    Represents the collections of all event types that are not transactions.

    This is meant to replace Events. They will both exist during the migration.
    """

    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.ERRORS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        self.__time_group_columns = {"time": "timestamp", "rtime": "received"}
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage, mappers=errors_translators
            ),
            abstract_column_set=columns,
            writable_storage=storage,
            time_group_columns=self.__time_group_columns,
            time_parse_columns=("timestamp", "received"),
        )

        self.__tags_processor = TagColumnProcessor(
            columns=columns,
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
        return processed_column or super().column_expr(
            column_name, query, parsing_context, table_alias
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
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
            HandledFunctionsProcessor(
                "exception_stacks.mechanism_handled", self.get_abstract_columnset()
            ),
        ]
