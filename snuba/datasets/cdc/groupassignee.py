from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.resolver import SingleTableResolver
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_cdc_storage
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor


class GroupAssigneeDataset(Dataset):
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
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            column_resolver=SingleTableResolver(schema.get_columns()),
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
