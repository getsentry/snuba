from snuba.utils.streams.codecs import Codec, PassthroughCodec


def test_passthrough_codec() -> None:
    codec: Codec[object, object] = PassthroughCodec()
    value = object()
    assert codec.decode(value) is value
    assert codec.encode(value) is value
