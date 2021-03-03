from typing import Mapping, Sequence

from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor


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
                query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
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
            required_filter_columns=None,
            required_time_column=None,
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            ProjectRateLimiterProcessor("project_id"),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}
