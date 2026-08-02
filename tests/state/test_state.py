from collections import ChainMap

from snuba.state import safe_dumps


def test_safe_dumps() -> None:
    assert safe_dumps(
        ChainMap({"a": 1}, {"b": 2}),
        sort_keys=True,
    ) == safe_dumps(
        {"a": 1, "b": 2},
        sort_keys=True,
    )
