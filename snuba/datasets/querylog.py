from typing import Mapping

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.dataset import Dataset
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_writable_storage
from snuba.query.extensions import QueryExtension


class QuerylogDataset(Dataset[ClickhouseQuery, ClickhouseQueryPlan]):
    def __init__(self) -> None:

        storage = get_writable_storage("querylog")
        columns = storage.get_table_writer().get_schema().get_columns()

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=columns,
            writable_storage=storage,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}
