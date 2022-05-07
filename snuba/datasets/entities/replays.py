from typing import Sequence

from snuba.clickhouse.columns import DateTime, UInt
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.replays import storage as replays_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.object_id_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityRequiredColumnValidator,
)
from snuba.utils.schemas import Column

replays_data_model = EntityColumnSet(
    [Column("project_id", UInt(64)), Column("timestamp", DateTime())]
)


class ReplaysEntity(Entity):
    def __init__(self) -> None:

        writable_storage = get_writable_storage(StorageKey.REPLAYS)

        super().__init__(
            storages=[writable_storage],
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    storage=replays_storage
                ),
            ),
            abstract_column_set=replays_data_model,
            join_relationships={},
            writable_storage=writable_storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="timestamp",
            validate_data_model=ColumnValidationMode.WARN,
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]
