from datetime import timedelta
from typing import Any, Mapping, Sequence, Tuple

from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.util import parse_datetime


class OutcomesRawEntity(Entity):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.OUTCOMES_RAW)

        self.__time_group_columns = {"time": "timestamp"}
        self.__time_parse_columns = ("timestamp",)
        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=storage.get_schema().get_columns(),
            writable_storage=None,
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

    # TODO: This needs to burned with fire, for so many reasons.
    # It's here now to reduce the scope of the initial entity changes
    # but can be moved to a processor if not removed entirely.
    def process_condition(
        self, condition: Tuple[str, str, Any]
    ) -> Tuple[str, str, Any]:
        lhs, op, lit = condition
        if (
            lhs in self.__time_parse_columns
            and op in (">", "<", ">=", "<=", "=", "!=")
            and isinstance(lit, str)
        ):
            lit = parse_datetime(lit)
        return lhs, op, lit
