from typing import Optional

import pytest

from snuba.pipeline.query_pipeline import (
    InvalidQueryPipelineResult,
    QueryPipelineData,
    QueryPipelineError,
    QueryPipelineResult,
    QueryPipelineStage,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer


class TestQueryPipelineStage(QueryPipelineStage[int, int]):
    def _process_data(self, pipe_input: QueryPipelineResult[int]) -> int:
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


def test_handle_error() -> None:
    error_processed = 0

    class ErrorStage(QueryPipelineStage[int, int]):
        def _process_error(self, pipe_input: QueryPipelineError[int]) -> int | Exception:
            nonlocal error_processed
            error_processed = 1
            return super()._process_error(pipe_input)

        def _process_data(self, pipe_input: QueryPipelineData[int]) -> int:
            raise Exception("Should not get here")

    input: QueryPipelineResult[int] = QueryPipelineResult(
        data=None,
        error=Exception("some BS"),
        query_settings=HTTPQuerySettings(),
        timer=Timer("something"),
    )
    res = ErrorStage().execute(input)
    assert error_processed
    assert res.error == input.error


def test_recover_from_error() -> None:
    ERROR_PROCESSED_RETURN = 42069

    class ErrorRecoverStage(QueryPipelineStage[int, int]):
        def _process_error(self, pipe_input: QueryPipelineError[int]) -> int | Exception:
            if isinstance(pipe_input.error, ValueError):
                return ERROR_PROCESSED_RETURN
            return super()._process_error(pipe_input)

        def _process_data(self, pipe_input: QueryPipelineData[int]) -> int:
            raise Exception("Should not get here")

    input: QueryPipelineResult[int] = QueryPipelineResult(
        data=None,
        error=Exception("some BS"),
        query_settings=HTTPQuerySettings(),
        timer=Timer("something"),
    )
    assert ErrorRecoverStage().execute(input).error == input.error
    input_expected_error: QueryPipelineResult[int] = QueryPipelineResult(
        data=None,
        error=ValueError("some BS"),
        query_settings=HTTPQuerySettings(),
        timer=Timer("something"),
    )
    assert ErrorRecoverStage().execute(input_expected_error).data == ERROR_PROCESSED_RETURN
