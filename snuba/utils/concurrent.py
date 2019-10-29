from typing import Optional, TypeVar
from typing_extensions import Protocol


T = TypeVar('T')


class Future(Protocol[T]):
    def result(self, timeout: Optional[float] = None) -> T:
        raise NotImplementedError

    def set_result(self, result: T) -> None:
        raise NotImplementedError

    def set_exception(self, exception: Exception) -> None:
        raise NotImplementedError
