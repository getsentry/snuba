from typing import Optional

import pytest

from snuba.pipeline.query_pipeline import (
    InvalidQueryPipelineResult,
    QueryPipelineResult,
    QueryPipelineStage,
)


class TestQueryPipelineStage(QueryPipelineStage[int, int]):
    def _execute(self, input: QueryPipelineResult[int]) -> QueryPipelineResult[int]:
        try:
            result = check_input_and_multiply(input.data)
            return QueryPipelineResult(result, None)
        except Exception as e:
            return QueryPipelineResult(None, e)


def check_input_and_multiply(num: Optional[int]) -> int:
    if num == 0 or num is None:
        raise Exception("Input cannot be zero")
    return num * 2


def test_query_pipeline_stage() -> None:
    input = QueryPipelineResult(data=1, error=None)
    result = TestQueryPipelineStage().execute(input)
    assert result.data == 2

    input = QueryPipelineResult(data=0, error=None)
    result = TestQueryPipelineStage().execute(input)
    assert str(result.error) == "Input cannot be zero"

    with pytest.raises(InvalidQueryPipelineResult):
        input = QueryPipelineResult(data=None, error=None)
