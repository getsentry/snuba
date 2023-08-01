from typing import cast

import pytest
import rapidjson

from snuba.reader import Result
from snuba.utils.serializable_exception import SerializableException
from snuba.web import QueryResult
from snuba.web.db_query import QueryResultCacheCodec


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
    codec = QueryResultCacheCodec()
    expected_decoded = QueryResult(
        result=result,
        extra={"stats": {"cache_hit": 1}, "sql": "fizz", "experiments": {}},
    )
    assert codec.decode(codec.encode(payload)) == expected_decoded


def test_decode_backwards_compatibility() -> None:
    result: Result = {
        "meta": [{"name": "foo", "type": "bar"}],
        "data": [{"foo": "bar"}],
        "totals": {"foo": 1},
    }
    # previous way of encoding
    encoded = cast(str, rapidjson.dumps(result, default=str)).encode("utf-8")
    codec = QueryResultCacheCodec()
    expected_decoded = QueryResult(
        result=result,
        extra={"stats": {"cache_hit": 1}, "sql": "", "experiments": {}},
    )
    assert codec.decode(encoded) == expected_decoded


def test_encode_decode_exception() -> None:
    class SomeException(SerializableException):
        pass

    codec = QueryResultCacheCodec()
    encoded_exception = codec.encode_exception(SomeException("some message"))
    with pytest.raises(SomeException):
        codec.decode(encoded_exception)
