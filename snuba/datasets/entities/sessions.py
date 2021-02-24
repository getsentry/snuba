from datetime import timedelta
from typing import Mapping, Sequence

from snuba import environment, state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToCurriedFunction,
    ColumnToFunction,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.conditions import (
    in_condition,
    ConditionFunctions,
    binary_condition,
    BooleanFunctions,
)
from snuba.query.expressions import Column, FunctionCall, Literal, Expression
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.logical import Query
from snuba.processor import MAX_UINT32, NIL_UUID
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.timeseries_processor import (
    TimeSeriesProcessor,
    extract_granularity_from_query,
)
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api.sessions")


def function_column(col_name: str, function_name: str) -> ColumnToFunction:
    return ColumnToFunction(
        None, col_name, function_name, (Column(None, None, col_name),),
    )


def function_call(col_name: str, function_name: str) -> FunctionCall:
    return FunctionCall(None, function_name, (Column(None, None, col_name),))


def plus_columns(
    col_name: str, col_a: FunctionCall, col_b: FunctionCall
) -> ColumnToFunction:
    return ColumnToFunction(None, col_name, "plus", (col_a, col_b))


# We have the following columns that we want to query:
# * duration_quantiles
# * duration_avg
# * sessions
# * sessions_crashed
# * sessions_abnormal
# * sessions_errored
# * users
# * users_crashed
# * users_abnormal
# * users_errored

quantiles = tuple(Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99, 1])

sessions_hourly_translators = TranslationMappers(
    columns=[
        ColumnToCurriedFunction(
            None,
            "duration_quantiles",
            FunctionCall(None, "quantilesIfMerge", quantiles),
            (Column(None, None, "duration_quantiles"),),
        ),
        function_column("duration_avg", "avgIfMerge"),
        plus_columns(
            "sessions",
            function_call("sessions", "countIfMerge"),
            function_call("sessions_preaggr", "sumIfMerge"),
        ),
        plus_columns(
            "sessions_crashed",
            function_call("sessions_crashed", "countIfMerge"),
            function_call("sessions_crashed_preaggr", "sumIfMerge"),
        ),
        plus_columns(
            "sessions_abnormal",
            function_call("sessions_abnormal", "countIfMerge"),
            function_call("sessions_abnormal_preaggr", "sumIfMerge"),
        ),
        plus_columns(
            "sessions_errored",
            function_call("sessions_errored", "uniqIfMerge"),
            function_call("sessions_errored_preaggr", "sumIfMerge"),
        ),
        function_column("users", "uniqIfMerge"),
        function_column("users_crashed", "uniqIfMerge"),
        function_column("users_abnormal", "uniqIfMerge"),
        function_column("users_errored", "uniqIfMerge"),
    ]
)

quantity = Column(None, None, "quantity")
seq = Column(None, None, "seq")
status = Column(None, None, "status")
session_id = Column(None, None, "session_id")
distinct_id = Column(None, None, "distinct_id")
duration = Column(None, None, "duration")
lit_nil = Literal(None, NIL_UUID)


def eq(col: Column, lit: Literal) -> FunctionCall:
    return binary_condition(ConditionFunctions.EQ, col, lit)


def neq(col: Column, lit: Literal) -> FunctionCall:
    return binary_condition(ConditionFunctions.NEQ, col, lit)


def _and(ex1: Expression, ex2: Expression) -> FunctionCall:
    return binary_condition(BooleanFunctions.AND, ex1, ex2)


# `errors > 0`
has_errors = binary_condition(
    ConditionFunctions.GT, Column(None, None, "errors"), Literal(None, 0)
)
# `distinct_id != NIL`
did_not_nil = neq(distinct_id, lit_nil)
# `duration != MAX AND status == 1`
duration_condition = _and(
    neq(duration, Literal(None, MAX_UINT32)), eq(status, Literal(None, 1))
)
# `status IN (2,3,4)`
terminal_status = in_condition(status, [Literal(None, status) for status in [2, 3, 4]])

# These here are basically the same statements as the matview query
sessions_raw_translators = TranslationMappers(
    columns=[
        ColumnToCurriedFunction(
            None,
            "duration_quantiles",
            FunctionCall(None, "quantilesIf", quantiles),
            (duration, duration_condition),
        ),
        ColumnToFunction(None, "duration_avg", "avgIf", (duration, duration_condition)),
        ColumnToFunction(
            None, "sessions", "sumIf", (quantity, eq(seq, Literal(None, 0))),
        ),
        ColumnToFunction(
            None, "sessions_crashed", "sumIf", (quantity, eq(status, Literal(None, 2)))
        ),
        ColumnToFunction(
            None, "sessions_abnormal", "sumIf", (quantity, eq(status, Literal(None, 3)))
        ),
        ColumnToFunction(
            None,
            "sessions_errored",
            "plus",
            (
                FunctionCall(
                    None,
                    "uniqIf",
                    (session_id, _and(has_errors, neq(session_id, lit_nil))),
                ),
                FunctionCall(
                    None,
                    "sumIf",
                    (quantity, _and(terminal_status, eq(session_id, lit_nil))),
                ),
            ),
        ),
        ColumnToFunction(None, "users", "uniqIf", (distinct_id, did_not_nil)),
        ColumnToFunction(
            None,
            "users_crashed",
            "uniqIf",
            (distinct_id, _and(eq(status, Literal(None, 2)), did_not_nil)),
        ),
        ColumnToFunction(
            None,
            "users_abnormal",
            "uniqIf",
            (distinct_id, _and(eq(status, Literal(None, 3)), did_not_nil)),
        ),
        ColumnToFunction(
            None,
            "users_errored",
            "uniqIf",
            (distinct_id, _and(has_errors, did_not_nil)),
        ),
    ]
)


class SessionsQueryStorageSelector(QueryStorageSelector):
    def __init__(self) -> None:
        self.materialized_storage = get_storage(StorageKey.SESSIONS_HOURLY)
        self.raw_storage = get_storage(StorageKey.SESSIONS_RAW)

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> StorageAndMappers:
        granularity = extract_granularity_from_query(query, "started") or 3600
        use_materialized_storage = granularity >= 3600 and (granularity % 3600) == 0

        metrics.increment(
            "query.selector",
            tags={
                "selected_storage": "materialized"
                if use_materialized_storage
                else "raw",
            },
        )

        allow_subhour_sessions = state.get_config("allow_subhour_sessions", 0)
        if not allow_subhour_sessions:
            use_materialized_storage = True

        if use_materialized_storage:
            return StorageAndMappers(
                self.materialized_storage, sessions_hourly_translators
            )
        else:
            return StorageAndMappers(self.raw_storage, sessions_raw_translators)


class SessionsEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.SESSIONS_RAW)
        materialized_storage = get_storage(StorageKey.SESSIONS_HOURLY)
        read_schema = materialized_storage.get_schema()

        super().__init__(
            storages=[writable_storage, materialized_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SelectedStorageQueryPlanBuilder(
                    selector=SessionsQueryStorageSelector()
                ),
            ),
            abstract_column_set=read_schema.get_columns(),
            join_relationships={},
            writable_storage=writable_storage,
            required_filter_columns=["org_id", "project_id"],
            required_time_column="started",
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
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]
