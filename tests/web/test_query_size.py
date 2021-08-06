import pytest

from snuba.web.query import MAX_QUERY_SIZE_BYTES, get_query_size_group

TENTH_PLUS_ONE = int(MAX_QUERY_SIZE_BYTES / 10) + 1
A = "A"

TEST_GROUPS = [
    pytest.param(A, ">=0%", id="Under 10%"),
    pytest.param(A * TENTH_PLUS_ONE, ">=10%", id="Greater than or equal to 10%"),
    pytest.param(A * TENTH_PLUS_ONE * 5, ">=50%", id="Greater than or equal to 50%"),
    pytest.param(A * TENTH_PLUS_ONE * 8, ">=80%", id="Greater than or equal to 80%"),
    pytest.param(A * TENTH_PLUS_ONE * 10, ">=100%", id="Greater than or equal to 100%"),
]


@pytest.mark.parametrize("query, group", TEST_GROUPS)
def test_query_size_group(query: str, group: str) -> None:
    assert get_query_size_group(query) == group
