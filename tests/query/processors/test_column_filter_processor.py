import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Literal
from snuba.query.processors.physical.column_filter_processor import (
    ColumnFilterProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings

TABLE = Table("issues", ColumnSet([]), storage_key=StorageKey("issues"))
VALID_COLUMNS = ["project_id", "occurrence_id"]

test_data = [
    pytest.param(
        Query(
            TABLE,
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    "equals", Column(None, None, VALID_COLUMNS[0]), Literal(None, 1)
                ),
                binary_condition(
                    "equals", Column(None, None, VALID_COLUMNS[1]), Literal(None, 1)
                ),
            ),
        ),
        False,
        id=f"Valid query, since it contains exactly {VALID_COLUMNS}",
    ),
    pytest.param(
        Query(
            TABLE,
            selected_columns=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    "equals", Column(None, None, VALID_COLUMNS[0]), Literal(None, 1)
                ),
                binary_condition(
                    "equals", Column(None, None, "event_id"), Literal(None, 1)
                ),
            ),
        ),
        False,
        id=f"Invalid query, since it doesn't contain exactly {VALID_COLUMNS}",
    ),
]


@pytest.mark.parametrize("query, valid", test_data)
@pytest.mark.redis_db
def test_condition_enforcer(query: Query, valid: bool) -> None:
    valid_columns = ["project_id", "occurrence_id"]
    query_settings = HTTPQuerySettings(consistent=True)
    processor = ColumnFilterProcessor(valid_columns)
    if valid:
        processor.process_query(query, query_settings)
    else:
        with pytest.raises(AssertionError):
            processor.process_query(query, query_settings)
