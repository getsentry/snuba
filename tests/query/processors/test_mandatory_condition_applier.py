import pytest
from typing import List

# from snuba.clickhouse.columns import ColumnSet, String

from snuba.datasets.schemas.tables import TableSource
from snuba.query.logical import Query
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier

from snuba.request.request_settings import HTTPRequestSettings
from snuba.datasets.schemas import MandatoryCondition
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal


test_data = [
    (
        "table1",
        [
            MandatoryCondition(
                ["deleted", "=", "0"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column(None, None, "deleted"),
                    Literal(None, 0),
                ),
            )
        ],
    ),
    (
        "table2",
        [
            MandatoryCondition(
                ["time", "=", "1"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column(None, None, "time"),
                    Literal(None, 0),
                ),
            ),
            MandatoryCondition(
                ["time2", "=", "2"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column(None, None, "time2"),
                    Literal(None, 0),
                ),
            ),
        ],
    ),
]


@pytest.mark.parametrize("table, mand_condition", test_data)
def test_mand_condition(table: str, mand_condition: List[MandatoryCondition]) -> None:

    body = {
        "conditions": [
            ["d", "=", "1"],
            ["c", "=", "3"],
            ["a", "=", "1"],
            ["b", "=", "2"],
        ],
    }

    # cols = ColumnSet([("col", String())])

    cols = None
    consistent = True

    query = Query(body, TableSource(table, cols, mand_condition, ["c1"]),)

    request_settings = HTTPRequestSettings(consistent=consistent)
    processor = MandatoryConditionApplier()
    processor.process_query(query, request_settings)

    assert query.get_conditions() == body["conditions"]
