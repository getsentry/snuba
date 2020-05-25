from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.schemas.resolver import SingleTableResolver
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension


class OutcomesDataset(TimeSeriesDataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:

        # The raw table we write onto, and that potentially we could
        # query.
        writable_storage = get_writable_storage(StorageKey.OUTCOMES_RAW)

        # The materialized view we query aggregate data from.
        materialized_storage = get_storage(StorageKey.OUTCOMES_HOURLY)
        read_schema = materialized_storage.get_schemas().get_read_schema()
        self.__time_group_columns = {"time": "timestamp"}
        super().__init__(
            storages=[writable_storage, materialized_storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                # TODO: Once we are ready to expose the raw data model and select whether to use
                # materialized storage or the raw one here, replace this with a custom storage
                # selector that decides when to use the materialized data.
                storage=materialized_storage,
            ),
            abstract_column_set=read_schema.get_columns(),
            writable_storage=writable_storage,
            column_resolver=SingleTableResolver(read_schema.get_columns()),
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

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]
