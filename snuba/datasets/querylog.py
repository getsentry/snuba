from typing import Mapping

from snuba.datasets.dataset import Dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.resolver import SingleTableResolver
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.query.extensions import QueryExtension


class QuerylogDataset(Dataset):
    def __init__(self) -> None:

        storage = get_writable_storage(StorageKey.QUERYLOG)
        columns = storage.get_table_writer().get_schema().get_columns()

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=columns,
            writable_storage=storage,
            column_resolver=SingleTableResolver(columns),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}
