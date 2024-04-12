from unittest.mock import MagicMock

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.clusters.cluster import _STORAGE_SET_CLUSTER_MAP, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_config_built_storages
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import StorageProcessingStage
from snuba.query.data_source.simple import Table
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.utils.metrics.timer import Timer


class MockQueryProcessor(ClickhouseQueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        return


# Create a fake storage and add it to global storages and clusters
mock_processors = [
    MockQueryProcessor(),
    MockQueryProcessor(),
    MockQueryProcessor(),
]
mock_storage = ReadableTableStorage(
    storage_key=StorageKey("mockstorage"),
    storage_set_key=StorageSetKey("mockstorageset"),
    schema=TableSchema(
        columns=ColumnSet([]),
        local_table_name="mockstoragelocal",
        dist_table_name="mockstoragedist",
        storage_set_key=StorageSetKey("mockstorageset"),
    ),
    readiness_state=ReadinessState.COMPLETE,
    query_processors=mock_processors,
    mandatory_condition_checkers=[],
    allocation_policies=[],
)
get_config_built_storages()[mock_storage.get_storage_key()] = mock_storage
_STORAGE_SET_CLUSTER_MAP[mock_storage.get_storage_set_key()] = CLUSTERS[0]


def test_basic():
    for f in mock_processors:
        f.process_query = MagicMock()

    ch_query = Query(
        from_clause=Table(
            "dontmatter",
            ColumnSet([]),
            storage_key=mock_storage.get_storage_key(),
        ),
        selected_columns=[],
        condition=None,
    )
    timer = Timer("test")
    settings = HTTPQuerySettings()
    res = StorageProcessingStage().execute(
        QueryPipelineResult(
            data=ch_query,
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )

    assert res.data
    fc = ch_query.get_from_clause()
    assert (
        fc
        and fc.table_name == mock_storage.get_schema().get_table_name()
        and fc.schema == mock_storage.get_schema().get_columns()
        and fc.storage_key == mock_storage.get_storage_key()
        and fc.allocation_policies == mock_storage.get_allocation_policies()
        and fc.mandatory_conditions == mock_storage.get_mandatory_condition_checkers()
    )
    for f in mock_processors:
        f.process_query.assert_called_with(ch_query, settings)
