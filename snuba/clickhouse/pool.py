from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypedDict

Params = Sequence[Any] | dict[str, Any] | None


class ClickhouseProfile(TypedDict):
    bytes: int
    progress_bytes: int
    blocks: int
    rows: int
    elapsed: float


@dataclass(frozen=True)
class ClickhouseResult:
    results: Sequence[Any] = field(default_factory=list)
    meta: Sequence[Any] | None = None
    profile: ClickhouseProfile | None = None
    trace_output: str = ""
    query_id: str = ""


class ClickhousePool(ABC):
    """HTTP ClickHouse connection. Concrete type is ClickhouseConnectPool."""

    host: str
    port: int
    user: str
    password: str
    database: str | None

    @abstractmethod
    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def command(
        self,
        statement: str,
        params: Params = None,
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def execute_explain(self, query: str) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def execute_with_totals(
        self,
        query: str,
        params: Params = None,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        capture_trace: bool = False,
        robust: bool = False,
    ) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def insert(
        self,
        table: str,
        data: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
