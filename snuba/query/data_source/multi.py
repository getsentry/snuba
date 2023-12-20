from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, Sequence

from snuba.query import ProcessableQuery, TSimpleDataSource
from snuba.query.data_source import DataSource
from snuba.utils.schemas import ColumnSet


@dataclass(frozen=True)
class MultiQuery(DataSource, Generic[TSimpleDataSource]):
    queries: Sequence[ProcessableQuery[TSimpleDataSource]]
    result_function: Callable[..., Any]

    def get_columns(self) -> ColumnSet:
        return self.queries[0].get_from_clause().get_columns()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MultiQuery):
            return False
        return self.queries == other.queries
