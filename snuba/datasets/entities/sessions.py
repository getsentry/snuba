from datetime import timedelta
from typing import Mapping, Sequence

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToCurriedFunction,
    ColumnToFunction,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension


def function_rule(col_name: str, function_name: str) -> ColumnToFunction:
    return ColumnToFunction(
        None, col_name, function_name, (Column(None, None, col_name),),
    )


class SessionsEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.SESSIONS_RAW)
        materialized_storage = get_storage(StorageKey.SESSIONS_HOURLY)
        read_schema = materialized_storage.get_schema()

        super().__init__(
            storages=[writable_storage, materialized_storage],
            # TODO: Once we are ready to expose the raw data model and select whether to use
            # materialized storage or the raw one here, replace this with a custom storage
            # selector that decides when to use the materialized data.
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=materialized_storage,
                mappers=TranslationMappers(
                    columns=[
                        ColumnToCurriedFunction(
                            None,
                            "duration_quantiles",
                            FunctionCall(
                                None,
                                "quantilesIfMerge",
                                (Literal(None, 0.5), Literal(None, 0.9)),
                            ),
                            (Column(None, None, "duration_quantiles"),),
                        ),
                        function_rule("sessions", "countIfMerge"),
                        function_rule("sessions_crashed", "countIfMerge"),
                        function_rule("sessions_abnormal", "countIfMerge"),
                        function_rule("users", "uniqIfMerge"),
                        function_rule("sessions_errored", "uniqIfMerge"),
                        function_rule("users_crashed", "uniqIfMerge"),
                        function_rule("users_abnormal", "uniqIfMerge"),
                        function_rule("users_errored", "uniqIfMerge"),
                    ]
                ),
            ),
            abstract_column_set=read_schema.get_columns(),
            writable_storage=writable_storage,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="started",
            ),
            "organization": OrganizationExtension(),
            "project": ProjectExtension(project_column="project_id"),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor(
                {"bucketed_started": "started"}, ("started", "received")
            ),
        ]
