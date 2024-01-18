import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.calculated_average_processor import (
    CalculatedAverageProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings

entity = get_entity(EntityKey("generic_metrics_gauges"))
query_entity = QueryEntity(EntityKey("generic_metrics_gauges"), entity.get_data_model())

AVG_VALUE_EXPRESSION = FunctionCall(
    alias="_snuba_aggregate_value",
    function_name="avg",
    parameters=(Column(alias="_snuba_value", table_name=None, column_name="value"),),
)

SUM_OVER_COUNT_EXPRESSION = FunctionCall(
    alias="_snuba_aggregate_value",
    function_name="divide",
    parameters=(
        FunctionCall(
            alias=None,
            function_name="sum",
            parameters=(
                Column(alias="_snuba_value", table_name=None, column_name="value"),
            ),
        ),
        FunctionCall(
            alias=None,
            function_name="count",
            parameters=(
                Column(alias="_snuba_value", table_name=None, column_name="value"),
            ),
        ),
    ),
)


@pytest.mark.parametrize(
    ("input_query", "expected_query"),
    [
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="aggregate_value", expression=AVG_VALUE_EXPRESSION
                    )
                ],
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="aggregate_value",
                        expression=SUM_OVER_COUNT_EXPRESSION,
                    ),
                ],
            ),
            id="simple select remapping",
        ),
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="timestamp",
                        expression=Column(
                            alias=None,
                            table_name=None,
                            column_name="timestamp",
                        ),
                    ),
                ],
                condition=FunctionCall(
                    None,
                    "greaterThan",
                    parameters=(AVG_VALUE_EXPRESSION, Literal(None, 5)),
                ),
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="timestamp",
                        expression=Column(
                            alias=None,
                            table_name=None,
                            column_name="timestamp",
                        ),
                    ),
                ],
                condition=FunctionCall(
                    None,
                    "greaterThan",
                    parameters=(SUM_OVER_COUNT_EXPRESSION, Literal(None, 5)),
                ),
            ),
            id="where clause with avg",
        ),
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="aggregate_value",
                        expression=FunctionCall(
                            alias="_snuba_aggregate_value",
                            function_name="min",
                            parameters=(
                                Column(
                                    alias="_snuba_value",
                                    table_name=None,
                                    column_name="value",
                                ),
                            ),
                        ),
                    )
                ],
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="aggregate_value",
                        expression=FunctionCall(
                            alias="_snuba_aggregate_value",
                            function_name="min",
                            parameters=(
                                Column(
                                    alias="_snuba_value",
                                    table_name=None,
                                    column_name="value",
                                ),
                            ),
                        ),
                    )
                ],
            ),
            id="no remap non-average functions",
        ),
    ],
)
def test_calculated_average_processor(input_query: Query, expected_query: Query):
    # TODO: Don't use the guages entity in this test, it shouldn't be necessary
    CalculatedAverageProcessor().process_query(input_query, HTTPQuerySettings())
    assert input_query == expected_query
