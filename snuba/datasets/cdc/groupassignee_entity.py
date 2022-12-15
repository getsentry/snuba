from typing import Sequence

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
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


class GroupAssigneeEntity(Entity):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    def __init__(self) -> None:
        storage = get_cdc_storage(StorageKey.GROUPASSIGNEES)
        schema = storage.get_table_writer().get_schema()

        super().__init__(
            storages=[storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=[StorageAndMappers(storage, TranslationMappers())]
                ),
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={
                "owns": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    columns=[("project_id", "project_id"), ("group_id", "group_id")],
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
