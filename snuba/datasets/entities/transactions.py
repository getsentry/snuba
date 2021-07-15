import random
from abc import ABC
from copy import deepcopy
from datetime import timedelta
from typing import List, Mapping, Optional, Sequence, Tuple

from snuba import environment, state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import EntityQueryPlanner, SimplePipelineBuilder
from snuba.query.conditions import get_literals_for_column_condition
from snuba.query.data_source.join import ColumnEquivalence, JoinRelationship, JoinType
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.performance_expressions import (
    apdex_processor,
    failure_rate_processor,
)
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.query.validation.validators import EntityRequiredColumnValidator
from snuba.request.request_settings import RequestSettings
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult

metrics = MetricsWrapper(environment.metrics, "snuplicator.sampling")


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
        ColumnToMapping(
            None,
            "geo_country_code",
            None,
            "contexts",
            "geo.country_code",
            nullable=True,
        ),
        ColumnToMapping(
            None, "geo_region", None, "contexts", "geo.region", nullable=True
        ),
        ColumnToMapping(None, "geo_city", None, "contexts", "geo.city", nullable=True),
    ],
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
        SubscriptableMapper(None, "measurements", None, "measurements", nullable=True),
        SubscriptableMapper(
            None, "span_op_breakdowns", None, "span_op_breakdowns", nullable=True
        ),
    ],
)


class SampledSimplePipelineBuilder(SimplePipelineBuilder):
    def build_planner(
        self, query: LogicalQuery, settings: RequestSettings,
    ) -> EntityQueryPlanner:
        new_query = deepcopy(query)
        new_query.set_sample(0.1)
        return super().build_planner(new_query, settings)


class BaseTransactionsEntity(Entity, ABC):
    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        storage = get_writable_storage(StorageKey.TRANSACTIONS)
        schema = storage.get_table_writer().get_schema()

        pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage,
                mappers=transaction_translator
                if custom_mappers is None
                else transaction_translator.concat(custom_mappers),
            ),
        )
        sampled_pipeline_builder = SampledSimplePipelineBuilder(
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=storage,
                mappers=transaction_translator
                if custom_mappers is None
                else transaction_translator.concat(custom_mappers),
            ),
        )

        def selector_func(query: LogicalQuery, referrer: str) -> Tuple[str, List[str]]:
            if referrer == "tagstore.__get_tag_keys":
                condition = query.get_condition()
                assert condition is not None
                project_literals = get_literals_for_column_condition(
                    "project_id", int, condition
                )
                values = set(lit.value for lit in project_literals)
                if 1 in values:
                    sample_query_rate = state.get_config(
                        "snuplicator_sampled_query", 0.0
                    )
                    assert isinstance(sample_query_rate, float)
                    if random.random() < sample_query_rate:
                        return "transactions", ["sampler"]

            return "transactions", []

        def callback_func(
            query: LogicalQuery,
            settings: RequestSettings,
            referrer: str,
            results: List[Result[QueryResult]],
        ) -> None:
            if not results:
                metrics.increment(
                    "query_result",
                    tags={"match": "empty", "primary": "none", "referrer": referrer},
                )
                return

            primary_result = results.pop(0)
            primary_function_id = primary_result.function_id
            secondary_result = results.pop(0) if len(results) > 0 else None

            # compare results
            comparison = "one_result"
            if secondary_result:
                primary_result_data = primary_result.result.result["data"]
                secondary_result_data = secondary_result.result.result["data"]

                primary_found_keys = set(row["tags_key"] for row in primary_result_data)
                secondary_found_keys = set(
                    row["tags_key"] for row in secondary_result_data
                )

                if primary_found_keys == secondary_found_keys:
                    comparison = "match"
                else:
                    comparison = "no_match"

            # Track which query(s) succeeded and the execution time
            metrics.increment(
                "query_result",
                tags={
                    "match": comparison,
                    "primary": primary_function_id,
                    "referrer": referrer,
                },
            )

        super().__init__(
            storages=[storage],
            query_pipeline_builder=PipelineDelegator(
                query_pipeline_builders={
                    "transactions": pipeline_builder,
                    "sampler": sampled_pipeline_builder,
                },
                selector_func=selector_func,
                callback_func=callback_func,
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={
                "contains": JoinRelationship(
                    rhs_entity=EntityKey.SPANS,
                    columns=[
                        ("project_id", "project_id"),
                        ("span_id", "transaction_span_id"),
                    ],
                    join_type=JoinType.INNER,
                    equivalences=[
                        ColumnEquivalence("event_id", "transaction_id"),
                        ColumnEquivalence("transaction_name", "transaction_name"),
                        ColumnEquivalence("trace_id", "trace_id"),
                    ],
                )
            },
            writable_storage=storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="finish_ts",
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
            TimeSeriesProcessor(
                {"time": "finish_ts"}, ("start_ts", "finish_ts", "timestamp")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            apdex_processor(self.get_data_model()),
            failure_rate_processor(self.get_data_model()),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]


class TransactionsEntity(BaseTransactionsEntity):
    pass
