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
from snuba.query.dsl import equals, literal
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.utils.metrics.timer import Timer


class MockQueryProcessor(ClickhouseQueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        query.add_condition_to_ast(equals(literal(1), literal(1)))


# Create a fake storage and add it to global storages and clusters
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
    query_processors=[MockQueryProcessor()],
    mandatory_condition_checkers=[],
    allocation_policies=[],
)
get_config_built_storages()[mock_storage.get_storage_key()] = mock_storage
_STORAGE_SET_CLUSTER_MAP[mock_storage.get_storage_set_key()] = CLUSTERS[0]


def test_basic() -> None:
    query = Query(
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
    schema = mock_storage.get_schema()
    assert isinstance(schema, TableSchema)
    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=query.get_from_clause().final,
                sampling_rate=query.get_from_clause().sampling_rate,
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[],
            condition=equals(literal(1), literal(1)),
        ),
        timer=timer,
        query_settings=settings,
        error=None,
    )
    actual = StorageProcessingStage().execute(
        QueryPipelineResult(
            data=query,
            timer=timer,
            query_settings=settings,
            error=None,
        )
    )
    assert actual == expected
