from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.resolver import SingleTableResolver
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension


class OutcomesRawDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.OUTCOMES_RAW)
        read_schema = storage.get_schemas().get_read_schema()

        self.__time_group_columns = {"time": "timestamp"}
        columns = read_schema.get_columns() + ColumnSet([("time", DateTime())])
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=columns,
            writable_storage=None,
            column_resolver=SingleTableResolver(columns),
            time_group_columns=self.__time_group_columns,
            time_parse_columns=("timestamp",),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="timestamp",
            ),
            "organization": OrganizationExtension(),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "org_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]
