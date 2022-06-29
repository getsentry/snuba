from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    ColumnSet,
    DateTime,
    Nested,
    UInt,
)
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.generic_metrics import sets_bucket_storage, sets_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class GenericMetricsSetsEntity(Entity):
    READABLE_STORAGE = sets_storage
    WRITABLE_STORAGE = sets_bucket_storage

    def __init__(self) -> None:
        super().__init__(
            storages=[sets_bucket_storage, sets_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    self.READABLE_STORAGE,
                    mappers=TranslationMappers(
                        subscriptables=[
                            SubscriptableMapper(None, "tags", None, "tags"),
                        ],
                        functions=[
                            FunctionNameMapper("uniq", "uniqCombined64Merge"),
                            FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
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
                    Column("bucketed_time", DateTime()),
                    Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ]
            ),
            join_relationships={},
            writable_storage=self.WRITABLE_STORAGE,
            validators=[EntityRequiredColumnValidator({"org_id", "project_id"})],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return []
