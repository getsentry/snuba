from typing import Sequence

from snuba.datasets.cdc import CdcDataset
from snuba.datasets.storages.groupedmessages import (
    POSTGRES_TABLE,
    schema,
    storage,
)
from snuba.datasets.plans.single_table import SingleTableQueryPlanBuilder
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.query_processor import QueryProcessor


class GroupedMessageDataset(CdcDataset):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

    def __init__(self) -> None:
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleTableQueryPlanBuilder(
                storage=storage, post_processors=[],
            ),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            default_control_topic="cdc_control",
            postgres_table=POSTGRES_TABLE,
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
