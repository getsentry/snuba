from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
)

if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    Column = TypedDict("Column", {"name": str, "type": str})
    Row = MutableMapping[str, Any]
    Result = TypedDict(
        "Result",
        {"meta": Sequence[Column], "data": Sequence[Row], "totals": Row},
        total=False,
    )
else:
    Result = MutableMapping[str, Any]


def iterate_rows(result: Result) -> Iterator[Row]:
    if "totals" in result:
        return itertools.chain(result["data"], [result["totals"]])
    else:
        return iter(result["data"])


def build_result_transformer(
    column_transformations: Mapping[Pattern, Callable[[Any], Any]],
) -> Callable[[Result], None]:
    """
    Builds and returns a function that can be used to mutate a ``Result``
    instance in-place by transforming all values for columns that have a
    transformation function specified for their data type.
    """

    def transform_result(result: Result) -> None:
        for column in result["meta"]:
            for pattern, transformer in column_transformations.items():
                if pattern.match(column["type"]):
                    name = column["name"]
                    for row in iterate_rows(result):
                        row[name] = transformer(row[name])
                    break

    return transform_result


TQuery = TypeVar("TQuery")


class Reader(ABC, Generic[TQuery]):
    @abstractmethod
    def execute(
        self,
        query: TQuery,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        """Execute a query."""
        raise NotImplementedError
