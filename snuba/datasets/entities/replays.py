from typing import Sequence

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.storage_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import StorageAndMappers
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
)
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityRequiredColumnValidator,
)


class ReplaysEntity(Entity):
    def __init__(self) -> None:
        writable_storage = get_writable_storage(StorageKey.REPLAYS)
        storage_and_mappers = [
            StorageAndMappers(writable_storage, TranslationMappers(), True)
        ]
        schema = writable_storage.get_table_writer().get_schema()

        super().__init__(
            storages=storage_and_mappers,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=StorageQueryPlanBuilder(
                    storage_and_mappers=storage_and_mappers, selector=None
                ),
            ),
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=writable_storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="timestamp",
            validate_data_model=ColumnValidationMode.WARN,
            subscription_processors=None,
            subscription_validators=None,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]
