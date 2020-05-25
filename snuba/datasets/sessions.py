from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.resolver import SingleTableResolver
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.parsing import ParsingContext
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor
from snuba.query.timeseries_extension import TimeSeriesExtension


class SessionsDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.SESSIONS_RAW)
        materialized_storage = get_storage(StorageKey.SESSIONS_HOURLY)
        read_schema = materialized_storage.get_schemas().get_read_schema()

        self.__time_group_columns = {"bucketed_started": "started"}
        columns = read_schema.get_columns() + ColumnSet(
            [("bucketed_started", DateTime())]
        )
        super().__init__(
            storages=[writable_storage, materialized_storage],
            # TODO: Once we are ready to expose the raw data model and select whether to use
            # materialized storage or the raw one here, replace this with a custom storage
            # selector that decides when to use the materialized data.
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=materialized_storage,
            ),
            abstract_column_set=columns,
            writable_storage=writable_storage,
            column_resolver=SingleTableResolver(columns),
            time_group_columns=self.__time_group_columns,
            time_parse_columns=("started", "received"),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="started",
            ),
            "organization": OrganizationExtension(),
            "project": ProjectExtension(
                processor=ProjectExtensionProcessor(project_column="project_id")
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        full_col = super().column_expr(column_name, query, parsing_context, table_alias)
        func = None
        if column_name == "duration_quantiles":
            func = "quantilesIfMerge(0.5, 0.9)"
        elif column_name in ("sessions", "sessions_crashed", "sessions_abnormal"):
            func = "countIfMerge"
        elif column_name in (
            "users",
            "sessions_errored",
            "users_crashed",
            "users_abnormal",
            "users_errored",
        ):
            func = "uniqIfMerge"
        if func is not None:
            return "{}({})".format(func, full_col)
        return full_col
