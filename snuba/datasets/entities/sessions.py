from typing import Sequence

from snuba import environment
from snuba.clickhouse.columns import ColumnSet, DateTime, UInt
from snuba.clickhouse.translators.legacy_mappers_we_cant_delete.sessions_mappers import (
    DurationAvgRawMapper,
    DurationQuantilesHourlyMapper,
    DurationQuantilesRawMapper,
    FunctionColumn,
    PlusFunctionColumns,
    SessionsRawCrashedMapper,
    SessionsRawErroredMapper,
    SessionsRawNumSessionsMapper,
    SessionsRawSessionsAbnormalMapper,
    SessionsRawUsersAbnormalMapper,
    SessionsRawUsersCrashedMapper,
    SessionsRawUsersErroredMapper,
    SessionsRawUsersMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.entities.storage_selectors.sessions import (
    SessionsQueryStorageSelector,
)
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.processors import AddColumnCondition
from snuba.datasets.entity_subscriptions.validators import AggregationValidator
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityRequiredColumnValidator,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api.sessions")


sessions_hourly_translators = TranslationMappers(
    columns=[
        DurationQuantilesHourlyMapper(),
        FunctionColumn("duration_avg", "avgIfMerge"),
        PlusFunctionColumns(
            "sessions",
            "sessions",
            "countIfMerge",
            "sessions_preaggr",
            "sumIfMerge",
        ),
        PlusFunctionColumns(
            "sessions_crashed",
            "sessions_crashed",
            "countIfMerge",
            "sessions_crashed_preaggr",
            "sumIfMerge",
        ),
        PlusFunctionColumns(
            "sessions_abnormal",
            "sessions_abnormal",
            "countIfMerge",
            "sessions_abnormal_preaggr",
            "sumIfMerge",
        ),
        PlusFunctionColumns(
            "sessions_errored",
            "sessions_errored",
            "uniqIfMerge",
            "sessions_errored_preaggr",
            "sumIfMerge",
        ),
        FunctionColumn("users", "uniqIfMerge"),
        FunctionColumn("users_crashed", "uniqIfMerge"),
        FunctionColumn("users_abnormal", "uniqIfMerge"),
        FunctionColumn("users_errored", "uniqIfMerge"),
    ]
)

# These here are basically the same statements as the matview query
sessions_raw_translators = TranslationMappers(
    columns=[
        DurationQuantilesRawMapper(),
        DurationAvgRawMapper(),
        SessionsRawNumSessionsMapper(),
        SessionsRawCrashedMapper(),
        SessionsRawSessionsAbnormalMapper(),
        SessionsRawErroredMapper(),
        SessionsRawUsersMapper(),
        SessionsRawUsersCrashedMapper(),
        SessionsRawUsersAbnormalMapper(),
        SessionsRawUsersErroredMapper(),
    ]
)


class SessionsEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.SESSIONS_RAW)
        materialized_storage = get_storage(StorageKey.SESSIONS_HOURLY)
        read_schema = materialized_storage.get_schema()
        storages = [
            EntityStorageConnection(
                materialized_storage, sessions_hourly_translators, False
            ),
            EntityStorageConnection(writable_storage, sessions_raw_translators, True),
        ]

        read_columns = read_schema.get_columns()
        time_columns = ColumnSet([("bucketed_started", DateTime())])
        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=storages,
                    selector=SessionsQueryStorageSelector(),
                ),
            ),
            abstract_column_set=read_columns + time_columns,
            join_relationships={},
            validators=[EntityRequiredColumnValidator(["org_id", "project_id"])],
            required_time_column="started",
            validate_data_model=ColumnValidationMode.WARN,
            subscription_processors=[AddColumnCondition("organization", "org_id")],
            subscription_validators=[
                AggregationValidator(2, ["groupby", "having", "orderby"], "started")
            ],
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor(
                {"bucketed_started": "started"}, ("started", "received")
            ),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]


class OrgSessionsEntity(Entity):
    def __init__(self) -> None:
        storage = get_storage(StorageKey.ORG_SESSIONS)
        storages = [EntityStorageConnection(storage, TranslationMappers(), False)]

        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storages=storages,
                    selector=DefaultQueryStorageSelector(),
                )
            ),
            abstract_column_set=ColumnSet(
                [
                    ("org_id", UInt(64)),
                    ("project_id", UInt(64)),
                    ("started", DateTime()),
                    ("bucketed_started", DateTime()),
                ]
            ),
            join_relationships={},
            validators=None,
            required_time_column="started",
            subscription_processors=None,
            subscription_validators=None,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor(
                {"bucketed_started": "started"}, ("started", "received")
            ),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]
