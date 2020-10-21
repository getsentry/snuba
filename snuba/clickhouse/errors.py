from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ClickhouseError(Exception):
    code: int
    message: str

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self}>"


@dataclass(frozen=True)
class ClickhouseWriterError(ClickhouseError):
    row: Optional[int] = None  # indexes start at 1
