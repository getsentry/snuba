from typing import Optional

import pytest

from snuba.pipeline.query_pipeline import (
    InvalidQueryPipelineResult,
    QueryPipelineResult,
    QueryPipelineStage,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer


class TestQueryPipelineStage(QueryPipelineStage[int, int]):
    def _process_data(
        self, pipe_input: QueryPipelineResult[int]
    ) -> QueryPipelineResult[int]:
        return check_input_and_multiply(pipe_input.data)


def check_input_and_multiply(num: Optional[int]) -> int:
    if num == 0 or num is None:
        raise Exception("Input cannot be zero")
    return num * 2


def test_query_pipeline_stage() -> None:
    input = QueryPipelineResult(
        data=1, error=None, query_settings=HTTPQuerySettings(), timer=Timer("something")
    )
    result = TestQueryPipelineStage().execute(input)
    assert result.data == 2

    input = QueryPipelineResult(
        data=0, error=None, query_settings=HTTPQuerySettings(), timer=Timer("something")
    )
    result = TestQueryPipelineStage().execute(input)
    assert str(result.error) == "Input cannot be zero"

    with pytest.raises(InvalidQueryPipelineResult):
        input = QueryPipelineResult(
            data=None,
            error=None,
            query_settings=HTTPQuerySettings(),
            timer=Timer("somethin"),
        )
