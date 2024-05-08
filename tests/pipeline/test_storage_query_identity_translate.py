from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storage import Storage
from snuba.pipeline.storage_query_identity_translate import try_translate_storage_query
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query


def test_translate_simple(
    mock_storage: Storage, mock_query_storage: QueryStorage
) -> None:
    input_query = Query(
        mock_query_storage,
        selected_columns=[
            SelectedExpression("trace_id", Column("_snuba_trace_id", None, "trace_id")),
            SelectedExpression(
                "duration", Column("_snuba_duration_ms", None, "duration_ms")
            ),
        ],
        granularity=None,
        condition=None,
        limit=100,
        offset=0,
    )
    storage_query = try_translate_storage_query(input_query)
    assert isinstance(storage_query, ClickhouseQuery)


def test_translate_composite(
    mock_storage: Storage, mock_query_storage: QueryStorage
) -> None:
    input_query = CompositeQuery(
        selected_columns=[
            SelectedExpression(
                "max_duration",
                FunctionCall(
                    "_snuba_max_duration",
                    "max",
                    (Column("_snuba_duration_ms", None, "_snuba_duration_ms"),),
                ),
            )
        ],
        from_clause=Query(
            mock_query_storage,
            selected_columns=[
                SelectedExpression(
                    "trace_id", Column("_snuba_trace_id", None, "trace_id")
                ),
                SelectedExpression(
                    "duration_ms", Column("_snuba_duration_ms", None, "duration_ms")
                ),
            ],
            granularity=None,
            condition=None,
            limit=100,
            offset=0,
        ),
        limit=100,
    )
    storage_query = try_translate_storage_query(input_query)
    assert isinstance(storage_query.get_from_clause().get_from_clause(), Table)  # type: ignore
