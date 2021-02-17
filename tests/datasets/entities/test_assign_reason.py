from typing import Any, Mapping, Sequence

import pytest

from datetime import datetime

from snuba.datasets.entities.assign_reason import assign_reason_category


now = datetime.now()


test_data = [
    pytest.param(
        [{"group_id": 1, "time": now, "aggregate": 2}],
        [{"group_id": 1, "time": now, "aggregate": 3}],
        "tsdb-modelid:4",
        "AGGREGATE_TOO_LOW",
        id="tsdb",
    ),
    pytest.param(
        [{"count": 4, "group_id": 2}, {"count": 4, "group_id": 4}],
        [{"count": 1, "group_id": 2}, {"count": 4, "group_id": 4}],
        "tagstore.get_groups_user_counts",
        "AGGREGATE_TOO_HIGH",
        id="tagstore.get_groups_user_counts",
    ),
    pytest.param([{"count": 1}], [{"count": 1}], "unknown", "UNKNOWN", id="default"),
]


@pytest.mark.parametrize("data, primary_data, referrer, expected_reason", test_data)
def test_assign_reason_category(
    data: Sequence[Mapping[str, Any]],
    primary_data: Sequence[Mapping[str, Any]],
    referrer: str,
    expected_reason: str,
) -> None:
    assert assign_reason_category(data, primary_data, referrer) == expected_reason
