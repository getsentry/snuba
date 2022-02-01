import pytest

from snuba.clickhouse.http import ValuesRowEncoder
from snuba.query.expressions import FunctionCall, Literal


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
