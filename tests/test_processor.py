from snuba.processor import _unicodify


def test_unicodify():
    # invalid utf-8 surrogate should be replaced with escape sequence
    assert _unicodify("\ud83c").encode("utf8") == b"\\ud83c"
