from typing import Sequence

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.storage_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import StorageAndMappers
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
)


class GroupedMessageEntity(Entity):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

    def __init__(self) -> None:
        storage = get_cdc_storage(StorageKey.GROUPEDMESSAGES)
        schema = storage.get_table_writer().get_schema()

        super().__init__(
            storages=[storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=[StorageAndMappers(storage, TranslationMappers(), True)]
                ),
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={
                "groups": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    columns=[("project_id", "project_id"), ("id", "group_id")],
                    join_type=JoinType.LEFT,
                    equivalences=[],
                )
            },
            writable_storage=storage,
            validators=None,
            required_time_column=None,
            subscription_processors=None,
            subscription_validators=None,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            ProjectRateLimiterProcessor("project_id"),
        ]
