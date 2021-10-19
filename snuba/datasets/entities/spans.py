from typing import Mapping, Sequence

from snuba.clickhouse.columns import UUID, ColumnSet, DateTime, Nested
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clickhouse.translators.snuba.mappers import SubscriptableMapper
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import ColumnEquivalence, JoinRelationship, JoinType
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator


class SpansEntity(Entity):
    def __init__(self) -> None:
        storage = get_writable_storage(StorageKey.SPANS)

        super().__init__(
            storages=[storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    storage=storage,
                    mappers=TranslationMappers(
                        subscriptables=[
                            SubscriptableMapper(None, "tags", None, "tags")
                        ],
                    ),
                ),
            ),
            abstract_column_set=ColumnSet(
                [
                    ("project_id", UInt(64)),
                    ("transaction_id", UUID()),
                    ("trace_id", UUID()),
                    ("transaction_span_id", UInt(64)),
                    ("span_id", UInt(64)),
                    ("parent_span_id", UInt(64, Modifiers(nullable=True))),
                    ("transaction_name", String()),
                    ("op", String()),
                    ("status", UInt(8)),
                    ("start_ts", DateTime()),
                    ("start_ns", UInt(32)),
                    ("finish_ts", DateTime()),
                    ("finish_ns", UInt(32)),
                    ("duration_ms", UInt(32)),
                    ("tags", Nested([("key", String()), ("value", String())])),
                ]
            ),
            join_relationships={
                "contained": JoinRelationship(
                    rhs_entity=EntityKey.TRANSACTIONS,
                    columns=[
                        ("project_id", "project_id"),
                        ("transaction_span_id", "span_id"),
                    ],
                    join_type=JoinType.INNER,
                    equivalences=[
                        ColumnEquivalence("transaction_id", "event_id"),
                        ColumnEquivalence("transaction_name", "transaction_name"),
                        ColumnEquivalence("trace_id", "trace_id"),
                    ],
                )
            },
            writable_storage=storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column=None,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
