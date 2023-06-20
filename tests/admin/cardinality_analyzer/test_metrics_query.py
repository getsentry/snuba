import pytest

from snuba.admin.cardinality_analyzer.cardinality_analyzer import _stringify_result
from snuba.clickhouse.native import ClickhouseResult


@pytest.mark.parametrize(
    "result,expected_result",
    [
        pytest.param(
            ClickhouseResult(
                [
                    [9223372036854776050, 909817],
                    [9223372036854776020, 909817],
                ]
            ),
            ClickhouseResult(
                [
                    ["9223372036854776050", "909817"],
                    ["9223372036854776020", "909817"],
                ]
            ),
            id="simple",
        ),
        pytest.param(
            ClickhouseResult(
                [
                    [[123, 456]],
                    [[567, 8910]],
                ]
            ),
            ClickhouseResult(
                [
                    ["[123, 456]"],
                    ["[567, 8910]"],
                ]
            ),
            id="simple",
        ),
    ],
)
def test_stringify_result(
    result: ClickhouseResult, expected_result: ClickhouseResult
) -> None:
    stringified_result = _stringify_result(result)
    assert stringified_result == expected_result
