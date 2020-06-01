from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToFunctionMapper,
    ColumnToLiteralMapper,
    ColumnToTagMapper,
    SimpleColumnMapper,
    TagMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.expressions import Column, FunctionCall
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.apdex_processor import ApdexProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.error_rate_processor import ErrorRateProcessor
from snuba.query.processors.impact_processor import ImpactProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension

# TODO: This will be a property of the relationship between entity and
# storage. Now we do not have entities so it is between dataset and
# storage.
transactions_translator = TranslationMappers(
    columns=[
        ColumnToFunctionMapper(
            None,
            "ip_address",
            "coalesce",
            (
                FunctionCall(
                    None, "IPv4NumToString", (Column(None, None, "ip_address_v4"),),
                ),
                FunctionCall(
                    None, "IPv6NumToString", (Column(None, None, "ip_address_v6"),),
                ),
            ),
        ),
        # These column aliases originally existed in the ``discover`` dataset,
        # but now live here to maintain compatibility between the composite
        # ``discover`` dataset and the standalone ``transaction`` dataset. In
        # the future, these aliases should be defined on the Transaction entity
        # instead of the dataset.
        ColumnToLiteralMapper(None, "type", "transaction"),
        SimpleColumnMapper(None, "timestamp", None, "finish_ts"),
        SimpleColumnMapper(None, "username", None, "user_name"),
        SimpleColumnMapper(None, "email", None, "user_email"),
        SimpleColumnMapper(None, "transaction", None, "transaction_name"),
        SimpleColumnMapper(None, "message", None, "transaction_name"),
        SimpleColumnMapper(None, "title", None, "transaction_name"),
        ColumnToTagMapper(
            None, "geo_country_code", None, "contexts", "geo.country_code"
        ),
        ColumnToTagMapper(None, "geo_region", None, "contexts", "geo.region"),
        ColumnToTagMapper(None, "geo_city", None, "contexts", "geo.city"),
    ],
    subscriptables=[
        TagMapper(None, "tags", None, "tags"),
        TagMapper(None, "contexts", None, "contexts"),
    ],
)


class TransactionsDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.TRANSACTIONS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        self.__tags_processor = TagColumnProcessor(
            columns=columns,
            promoted_columns=self._get_promoted_columns(),
            column_tag_map=self._get_column_tag_map(),
        )
        self.__time_group_columns = {
            "time": "finish_ts",
        }
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage, mappers=transactions_translator
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            time_group_columns=self.__time_group_columns,
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
                timestamp_column="finish_ts",
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

        # These column aliases originally existed in the ``discover`` dataset,
        # but now live here to maintain compatibility between the composite
        # ``discover`` dataset and the standalone ``transaction`` dataset. In
        # the future, these aliases should be defined on the Transaction entity
        # instead of the dataset.
        if column_name == "type":
            return "'transaction'"
        if column_name == "timestamp":
            return "finish_ts"
        if column_name == "username":
            return "user_name"
        if column_name == "email":
            return "user_email"
        if column_name == "transaction":
            return "transaction_name"
        if column_name == "message":
            return "transaction_name"
        if column_name == "title":
            return "transaction_name"

        if column_name == "geo_country_code":
            column_name = "contexts[geo.country_code]"
        if column_name == "geo_region":
            column_name = "contexts[geo.region]"
        if column_name == "geo_city":
            column_name = "contexts[geo.city]"

        processed_column = self.__tags_processor.process_column_expression(
            column_name, query, parsing_context, table_alias
        )
        if processed_column:
            # If processed_column is None, this was not a tag/context expression
            return processed_column
        return super().column_expr(column_name, query, parsing_context)

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            ApdexProcessor(),
            ImpactProcessor(),
            ErrorRateProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]
