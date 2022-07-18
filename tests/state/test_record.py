from snuba.state import _kafka_producer


def test_get_producer():
    assert _kafka_producer() is not None
