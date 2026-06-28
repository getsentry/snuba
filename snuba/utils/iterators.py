from collections.abc import Iterable, Iterator, MutableSequence, Sequence
from typing import TypeVar

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Iterator[Sequence[T]]:
    chunk: MutableSequence[T] = []

    for value in iterable:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
