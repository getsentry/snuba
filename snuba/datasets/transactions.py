from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_table import (
    SimpleQueryPlanExecutionStrategy,
    SingleTableQueryPlanBuilder,
)
from snuba.datasets.storages.transactions import (
    columns,
    schema,
    storage,
)
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query_processor import QueryProcessor
from snuba.query.processors.apdex_processor import ApdexProcessor
from snuba.query.processors.impact_processor import ImpactProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query import Query
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor
from snuba.web.split import (
    ColumnSplitSpec,
    SplitQueryPlanExecutionStrategy,
)


class TransactionsDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        self.__tags_processor = TagColumnProcessor(
            columns=columns,
            promoted_columns=self._get_promoted_columns(),
            column_tag_map=self._get_column_tag_map(),
        )

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleTableQueryPlanBuilder(
                storage=storage,
                post_processors=[PrewhereProcessor()],
                execution_strategy=SplitQueryPlanExecutionStrategy(
                    ColumnSplitSpec(
                        id_column="event_id",
                        project_column="project_id",
                        timestamp_column="start_ts",
                    ),
                    default_strategy=SimpleQueryPlanExecutionStrategy(),
                ),
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            time_group_columns={
                "bucketed_start": "start_ts",
                "bucketed_end": "finish_ts",
            },
            time_parse_columns=("start_ts", "finish_ts"),
        )

    def _get_promoted_columns(self):
        # TODO: Support promoted tags
        return {
            "tags": frozenset(),
            "contexts": frozenset(),
        }

    def _get_column_tag_map(self):
        # TODO: Support promoted tags
        return {
            "tags": {},
            "contexts": {},
        }

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectExtensionProcessor(project_column="project_id")
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="start_ts",
            ),
        }

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        # TODO remove these casts when clickhouse-driver is >= 0.0.19
        if column_name == "ip_address_v4":
            return "IPv4NumToString(ip_address_v4)"
        if column_name == "ip_address_v6":
            return "IPv6NumToString(ip_address_v6)"
        if column_name == "ip_address":
            return f"coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))"
        if column_name == "event_id":
            return "replaceAll(toString(event_id), '-', '')"
        processed_column = self.__tags_processor.process_column_expression(
            column_name, query, parsing_context, table_alias
        )
        if processed_column:
            # If processed_column is None, this was not a tag/context expression
            return processed_column
        return super().column_expr(column_name, query, parsing_context)

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            ApdexProcessor(),
            ImpactProcessor(),
        ]
