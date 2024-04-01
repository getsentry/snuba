import pytest

from snuba import settings
from snuba.pipeline.stages.query_execution import get_query_size_group

TENTH_PLUS_ONE = int(settings.MAX_QUERY_SIZE_BYTES / 10) + 1
A = "A"

TEST_GROUPS = [
    pytest.param(1, ">=0%", id="Under 10%"),
    pytest.param(TENTH_PLUS_ONE, ">=10%", id="Greater than or equal to 10%"),
    pytest.param(TENTH_PLUS_ONE * 5, ">=50%", id="Greater than or equal to 50%"),
    pytest.param(TENTH_PLUS_ONE * 8, ">=80%", id="Greater than or equal to 80%"),
    pytest.param(
        settings.MAX_QUERY_SIZE_BYTES, "100%", id="Greater than or equal to 100%"
    ),
]


@pytest.mark.parametrize("query_size, group", TEST_GROUPS)
def test_query_size_group(query_size: int, group: str) -> None:
    assert get_query_size_group(query_size) == group
