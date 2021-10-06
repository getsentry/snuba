import pytest

from snuba.utils.serializable_exception import SerializableException
from snuba.web.db_query import ResultCacheCodec


def test_encode_decode():
    payload = {
        "meta": [{"name": "foo", "type": "bar"}],
        "data": [{"foo": "bar"}],
        "totals": {"foo": 1},
    }
    codec = ResultCacheCodec()
    assert codec.decode(codec.encode(payload)) == payload


def test_encode_decode_exception():
    class SomeException(SerializableException):
        pass

    codec = ResultCacheCodec()
    encoded_exception = codec.encode_exception(SomeException("some message"))
    with pytest.raises(SomeException):
        codec.decode(encoded_exception)
