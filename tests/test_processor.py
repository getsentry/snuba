from typing import cast

from snuba.processor import _unicodify


def test_unicodify() -> None:
    # invalid utf-8 surrogate should be replaced with escape sequence
    assert cast(str, _unicodify("\ud83c")).encode("utf8") == b"\\ud83c"
