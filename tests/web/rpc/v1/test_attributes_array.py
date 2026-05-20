import pytest

from snuba.web.rpc.common.common import pop_attributes_array_paths


@pytest.mark.parametrize(
    ("path", "raw", "expected"),
    [
        ("gen_ai.tool.input", '"{\\"location\\": \\"Paris\\"}"', '{"location": "Paris"}'),
        ("gen_ai.tool.call.arguments", '{"location": "Paris"}', '{"location": "Paris"}'),
    ],
)
def test_pop_attributes_array_paths_returns_stringified_non_list_values(
    path: str, raw: str, expected: str
) -> None:
    row = {path: raw}

    assert list(pop_attributes_array_paths(row)) == [(path, expected)]
    assert row == {}
