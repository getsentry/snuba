from typing import Mapping, Sequence

from snuba.datasets.entity import Entity
from snuba.pipeline.single_query_plan_pipeline import SingleQueryPlanPipelineBuilder
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor


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
            query_pipeline_builder=SingleQueryPlanPipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=storage,
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}
