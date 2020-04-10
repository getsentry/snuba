from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.query_processor import QueryProcessor


class GroupedMessageDataset(Dataset):
    """
    This is a clone of the bare minimum fields we need from postgres groupedmessage table
    to replace such a table in event search.
    """

    def __init__(self) -> None:
        storage = get_cdc_storage("groupedmessages")
        schema = storage.get_table_writer().get_schema()

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
        )

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
