from snuba.utils.iterators import chunked


def test_chunked() -> None:
    assert [*chunked([], 3)] == []
    assert [*chunked(range(3), 3)] == [[0, 1, 2]]
    assert [*chunked(range(10), 3)] == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
