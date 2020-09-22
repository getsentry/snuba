from datetime import timedelta
from typing import Any, FrozenSet, Mapping, Sequence, Tuple, Union

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    ColumnToColumn,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.performance_expressions import (
    apdex_processor,
    failure_rate_processor,
)
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.util import parse_datetime

# TODO: This will be a property of the relationship between entity and
# storage. Now we do not have entities so it is between dataset and
# storage.
transaction_translator = TranslationMappers(
    columns=[
        ColumnToFunction(
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
        ColumnToFunction(
            None, "user", "nullIf", (Column(None, None, "user"), Literal(None, ""))
        ),
        # These column aliases originally existed in the ``discover`` dataset,
        # but now live here to maintain compatibility between the composite
        # ``discover`` dataset and the standalone ``transaction`` dataset. In
        # the future, these aliases should be defined on the Transaction entity
        # instead of the dataset.
        ColumnToLiteral(None, "type", "transaction"),
        ColumnToColumn(None, "timestamp", None, "finish_ts"),
        ColumnToColumn(None, "username", None, "user_name"),
        ColumnToColumn(None, "email", None, "user_email"),
        ColumnToColumn(None, "transaction", None, "transaction_name"),
        ColumnToColumn(None, "message", None, "transaction_name"),
        ColumnToColumn(None, "title", None, "transaction_name"),
        ColumnToMapping(None, "geo_country_code", None, "contexts", "geo.country_code"),
        ColumnToMapping(None, "geo_region", None, "contexts", "geo.region"),
        ColumnToMapping(None, "geo_city", None, "contexts", "geo.city"),
    ],
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
        SubscriptableMapper(None, "measurements", None, "measurements", nullable=True),
    ],
)


class TransactionsEntity(Entity):
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
        self.__time_parse_columns = ("start_ts", "finish_ts")
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage, mappers=transaction_translator
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
        )

    def _get_promoted_columns(self) -> Mapping[str, FrozenSet[str]]:
        # TODO: Support promoted tags
        return {
            "tags": frozenset(),
            "contexts": frozenset(),
        }

    def _get_column_tag_map(self) -> Mapping[str, Mapping[str, str]]:
        # TODO: Support promoted tags
        return {
            "tags": {},
            "contexts": {},
        }

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="finish_ts",
            ),
        }

    def column_expr(
        self,
        column_name: str,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> Union[None, Any]:
        if column_name == "ip_address":
            return "coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))"
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
            apdex_processor(self.get_data_model()),
            failure_rate_processor(self.get_data_model()),
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
