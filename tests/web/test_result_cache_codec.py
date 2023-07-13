import pytest

from snuba.reader import Result
from snuba.utils.serializable_exception import SerializableException
from snuba.web import QueryResult
from snuba.web.db_query import ResultCacheCodec


def test_encode_decode() -> None:
    result: Result = {
        "meta": [{"name": "foo", "type": "bar"}],
        "data": [{"foo": "bar"}],
        "totals": {"foo": 1},
    }
    payload = QueryResult(
        result=result,
        extra={"stats": {}, "sql": "fizz", "experiments": {}},
    )
    codec = ResultCacheCodec()
    expected_decoded = QueryResult(
        result=result,
        extra={"stats": {"cache_hit": 1}, "sql": "fizz", "experiments": {}},
    )
    assert codec.decode(codec.encode(payload)) == expected_decoded


def test_encode_decode_exception() -> None:
    class SomeException(SerializableException):
        pass

    codec = ResultCacheCodec()
    encoded_exception = codec.encode_exception(SomeException("some message"))
    with pytest.raises(SomeException):
        codec.decode(encoded_exception)
