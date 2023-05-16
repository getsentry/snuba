from unittest.mock import Mock

import pytest

from snuba.clickhouse.http import HTTPWriteBatch, InsertStatement, ValuesRowEncoder
from snuba.query.expressions import FunctionCall, Literal
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


@pytest.fixture
def values_encoder() -> ValuesRowEncoder:
    return ValuesRowEncoder(["col1", "col2", "col3"])


def test_encode_preserves_column_order(values_encoder: ValuesRowEncoder) -> None:
    encoded = values_encoder.encode(
        {
            "col2": Literal(None, 5),
            "col3": Literal(None, "test_string"),
            "col1": FunctionCall(
                None,
                "test",
                tuple(
                    [FunctionCall(None, "inner", tuple([Literal(None, "inner_arg")]))]
                ),
            ),
        }
    )
    assert encoded == "(test(inner('inner_arg')),5,'test_string')".encode("utf-8")


def test_encode_fails_on_non_expression(values_encoder: ValuesRowEncoder) -> None:
    with (pytest.raises(TypeError)):
        values_encoder.encode({"col1": "string not wrapped by a literal object"})


def test_http_write_batch_raises_exception_on_timeout() -> None:
    # create a mock executor and connection pool
    executor = Mock()
    pool = Mock()

    # create an instance of HTTPWriteBatch with a very short timeout
    batch = HTTPWriteBatch(
        executor=executor,
        pool=pool,
        metrics=DummyMetricsBackend(),
        user="user",
        password="password",
        statement=InsertStatement(table_name="table"),
        encoding=None,
        options={},
        chunk_size=None,
        buffer_size=0,
        debug_buffer_size_bytes=None,
    )
    batch._result = Mock()
    batch._result.result = Mock(side_effect=TimeoutError())

    with pytest.raises(TimeoutError):
        batch.join(timeout=0.1)
