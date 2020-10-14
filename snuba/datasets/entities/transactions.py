from datetime import timedelta
from typing import Mapping, Optional, Sequence

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
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.performance_expressions import (
    apdex_processor,
    failure_rate_processor,
)
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension

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
    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        storage = get_writable_storage(StorageKey.TRANSACTIONS)
        schema = storage.get_table_writer().get_schema()

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage,
                mappers=transaction_translator
                if custom_mappers is None
                else transaction_translator.concat(custom_mappers),
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="finish_ts",
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            TimeSeriesProcessor({"time": "finish_ts"}, ("start_ts", "finish_ts")),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            apdex_processor(self.get_data_model()),
            failure_rate_processor(self.get_data_model()),
        ]
