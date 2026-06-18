import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.quantity_64_processor import Quantity64Processor
from snuba.query.query_settings import HTTPQuerySettings

entity = get_entity(EntityKey("outcomes_raw"))
query_entity = QueryEntity(EntityKey("outcomes_raw"), entity.get_data_model())


def quantity_column(column_name: str) -> Column:
    return Column(alias="_snuba_quantity", table_name=None, column_name=column_name)


@pytest.mark.parametrize(
    ("input_query", "expected_query"),
    [
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="quantity",
                        expression=FunctionCall(
                            alias="_snuba_sum_quantity",
                            function_name="sum",
                            parameters=(quantity_column("quantity"),),
                        ),
                    )
                ],
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="quantity",
                        expression=FunctionCall(
                            alias="_snuba_sum_quantity",
                            function_name="sum",
                            parameters=(quantity_column("quantity64"),),
                        ),
                    )
                ],
            ),
            id="remap quantity in select",
        ),
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="timestamp",
                        expression=Column(alias=None, table_name=None, column_name="timestamp"),
                    ),
                ],
                condition=FunctionCall(
                    None,
                    "greaterThan",
                    parameters=(quantity_column("quantity"), Literal(None, 5)),
                ),
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="timestamp",
                        expression=Column(alias=None, table_name=None, column_name="timestamp"),
                    ),
                ],
                condition=FunctionCall(
                    None,
                    "greaterThan",
                    parameters=(quantity_column("quantity64"), Literal(None, 5)),
                ),
            ),
            id="remap quantity in where clause",
        ),
        pytest.param(
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="quantity64",
                        expression=quantity_column("quantity64"),
                    ),
                    SelectedExpression(
                        name="category",
                        expression=Column(alias=None, table_name=None, column_name="category"),
                    ),
                ],
            ),
            Query(
                query_entity,
                selected_columns=[
                    SelectedExpression(
                        name="quantity64",
                        expression=quantity_column("quantity64"),
                    ),
                    SelectedExpression(
                        name="category",
                        expression=Column(alias=None, table_name=None, column_name="category"),
                    ),
                ],
            ),
            id="leave other columns untouched",
        ),
    ],
)
def test_quantity_64_processor(input_query: Query, expected_query: Query) -> None:
    Quantity64Processor().process_query(input_query, HTTPQuerySettings())
    assert input_query == expected_query
