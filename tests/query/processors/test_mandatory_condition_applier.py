import pytest


# from snuba.clickhouse.columns import ColumnSet, String

from snuba.datasets.schemas.tables import TableSource
from snuba.query.logical import Query
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier

from snuba.request.request_settings import HTTPRequestSettings


test_data = [
    ("table1", [["time1", "=", "1"]]),
    ("table2", [["time2", "=", "2"]]),
]


@pytest.mark.parametrize("table, mand_condition", test_data)
def test_prewhere(table, mand_condition) -> None:

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
    processor = MandatoryConditionApplier("sentry_dist", "sentry_ro")
    processor.process_query(query, request_settings)

    source = query.get_data_source()

    assert isinstance(source, TableSource)
    assert source.get_columns() == cols
    assert source.get_prewhere_candidates() == ["c1"]

    # want to check that the mandatory condition from TableSource object was added
    assert source.get_mandatory_conditions() == body["conditions"].append(
        mand_condition[0]
    )
