from typing import Mapping, Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    ColumnSet,
    DateTime,
    Nested,
    UInt,
)
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToFunction,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression, Literal, SubscriptableReference
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator
from snuba.request.request_settings import RequestSettings


class TagsTypeTransformer(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            if not isinstance(exp, SubscriptableReference):
                return exp

            key = exp.key
            if not isinstance(key.value, str) or not key.value.isdigit():
                raise InvalidExpressionException(
                    exp, "Expected a string key containing an integer in subscriptable."
                )

            return SubscriptableReference(
                exp.alias, exp.column, Literal(None, int(key.value))
            )

        query.transform_expressions(transform_expression)


class MetricsSetsEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.METRICS_BUCKETS)
        readable_storage = get_storage(StorageKey.METRICS_SETS)

        super().__init__(
            # TODO: Add the readable storages
            storages=[writable_storage, readable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    readable_storage,
                    mappers=TranslationMappers(
                        columns=[
                            ColumnToFunction(
                                None,
                                "value",
                                "uniqCombined64Merge",
                                (ColumnExpr(None, None, "value"),),
                            ),
                        ],
                        subscriptables=[
                            SubscriptableMapper(None, "tags", None, "tags"),
                        ],
                    ),
                )
            ),
            abstract_column_set=ColumnSet(
                [
                    Column("org_id", UInt(64)),
                    Column("project_id", UInt(64)),
                    Column("metric_id", UInt(64)),
                    Column("timestamp", DateTime()),
                    Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
                    Column(
                        "value", AggregateFunction("uniqCombined64Merge", [UInt(64)])
                    ),
                ]
            ),
            join_relationships={},
            writable_storage=writable_storage,
            validators=[EntityRequiredColumnValidator({"org_id", "project_id"})],
            required_time_column="timestamp",
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            ProjectRateLimiterProcessor(project_column="project_id"),
            TagsTypeTransformer(),
        ]
