import uuid

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Query
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.pipeline.query_pipeline import QueryPipelineData, QueryPipelineResult
from snuba.pipeline.stages.query_processing import EntityProcessingStage
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query_initial
from snuba.request import Request
from snuba.utils.metrics.timer import Timer

"""
Create a fake storage and entity, put the entity in global definitions
"""


def _create_pipe_input_from_snql(snql_query: str) -> QueryPipelineData[Request]:
    query_body = {"query": snql_query}
    logical_query = parse_snql_query_initial(query_body["query"])
    query_settings = HTTPQuerySettings()
    timer = Timer("test")
    request = Request(
        id=uuid.uuid4(),
        original_body=query_body,
        query=logical_query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    return QueryPipelineData(
        data=request,
        query_settings=request.query_settings,
        timer=timer,
        error=None,
    )


def test_basic(
    mock_storage: ReadableTableStorage, mock_entity: PluggableEntity
) -> None:
    pipe_input = _create_pipe_input_from_snql(
        f"MATCH ({mock_entity.entity_key.value}) "
        "SELECT timestamp "
        "WHERE "
        "org_id = 1 AND "
        "project_id = 1"
    )

    logical_query = pipe_input.data.query
    actual = EntityProcessingStage().execute(pipe_input)
    schema = mock_storage.get_schema()
    assert isinstance(schema, TableSchema)

    assert isinstance(logical_query, LogicalQuery)
    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=logical_query.get_final(),
                sampling_rate=logical_query.get_sample(),
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[SelectedExpression("timestamp", column("timestamp"))],
            condition=and_cond(
                equals(literal(1), literal(1)),
                and_cond(
                    equals(column("org_id"), literal(1)),
                    equals(column("project_id"), literal(1)),
                ),
            ),
            limit=1000,
        ),
        query_settings=pipe_input.query_settings,
        timer=pipe_input.timer,
        error=None,
    )
    assert actual == expected


def test_basic_storage(
    mock_storage: ReadableTableStorage, mock_query_storage: QueryStorage
) -> None:
    pipe_input = _create_pipe_input_from_snql(
        f"MATCH STORAGE({mock_query_storage.key.value}) "
        "SELECT timestamp "
        "WHERE "
        "org_id = 1 AND "
        "project_id = 1"
    )

    schema = mock_storage.get_schema()
    actual = EntityProcessingStage().execute(pipe_input)
    logical_query = pipe_input.data.query
    assert isinstance(logical_query, LogicalQuery)
    assert isinstance(schema, TableSchema)

    expected = QueryPipelineResult(
        data=Query(
            from_clause=Table(
                table_name=schema.get_table_name(),
                schema=mock_storage.get_schema().get_columns(),
                storage_key=mock_storage.get_storage_key(),
                allocation_policies=mock_storage.get_allocation_policies(),
                final=logical_query.get_final(),
                sampling_rate=logical_query.get_sample(),
                mandatory_conditions=mock_storage.get_schema()
                .get_data_source()
                .get_mandatory_conditions(),
            ),
            selected_columns=[SelectedExpression("timestamp", column("timestamp"))],
            condition=and_cond(
                # NOTE: no query processor was applied here, this is because the storage query does not run
                # entity processors (there is no entity)
                equals(column("org_id"), literal(1)),
                equals(column("project_id"), literal(1)),
            ),
            limit=1000,
        ),
        query_settings=pipe_input.query_settings,
        timer=pipe_input.timer,
        error=None,
    )
    assert repr(actual.data) == repr(expected.data)
    assert actual == expected
