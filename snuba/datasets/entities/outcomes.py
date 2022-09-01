from typing import Sequence

from snuba.clickhouse.columns import DateTime, String, UInt
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityRequiredColumnValidator,
)
from snuba.utils.schemas import Column

outcomes_data_model = EntityColumnSet(
    [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("key_id", UInt(64)),
        Column("timestamp", DateTime()),
        Column("outcome", UInt(8)),
        Column("reason", String()),
        Column("quantity", UInt(64)),
        Column("category", UInt(8)),
        Column("times_seen", UInt(64)),
        Column("time", DateTime()),
    ]
)


class OutcomesEntity(Entity):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:

        # The raw table we write onto, and that potentially we could
        # query.
        writable_storage = get_writable_storage(StorageKey.OUTCOMES_RAW)
        # The materialized view we query aggregate data from.
        materialized_storage = get_storage(StorageKey.OUTCOMES_HOURLY)

        super().__init__(
            storages=[writable_storage, materialized_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    # TODO: Once we are ready to expose the raw data model and select whether to use
                    # materialized storage or the raw one here, replace this with a custom storage
                    # selector that decides when to use the materialized data.
                    storage=materialized_storage,
                ),
            ),
            abstract_column_set=outcomes_data_model,
            join_relationships={},
            writable_storage=writable_storage,
            validators=[EntityRequiredColumnValidator({"org_id"})],
            required_time_column="timestamp",
            # WARN mode logged way too many events to Sentry
            validate_data_model=ColumnValidationMode.WARN,
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
        ]
