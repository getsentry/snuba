from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Pattern,
    Sequence,
    Tuple,
    TypedDict,
    TypeVar,
)

from snuba.clickhouse.formatter.nodes import FormattedQuery

Column = TypedDict("Column", {"name": str, "type": str}, total=False)
Row = Dict[str, Any]

Result = TypedDict(
    "Result",
    {
        "meta": List[Column],
        "data": List[Row],
        "totals": Row,
        "profile": Optional[Dict[str, Any]],
        "trace_output": str,
    },
    total=False,
)


def iterate_rows(result: Result) -> Iterator[Row]:
    if "totals" in result:
        return itertools.chain(result["data"], [result["totals"]])
    else:
        return iter(result["data"])


def transform_rows(result: Result, transformer: Callable[[Row], Row]) -> None:
    """
    Transforms the Result dictionary in place replacing each Row object
    with the one returned by the transformer function.
    """
    for index, row in enumerate(result["data"]):
        result["data"][index] = transformer(row)

    if "totals" in result:
        result["totals"] = transformer(result["totals"])


NULLABLE_RE = re.compile(r"^Nullable\((.+)\)$")


def unwrap_nullable_type(type: str) -> Tuple[bool, str]:
    match = NULLABLE_RE.match(type)
    if match is not None:
        return True, match.groups()[0]
    else:
        return False, type


T = TypeVar("T")
R = TypeVar("R")


def transform_nullable(
    function: Callable[[T], R]
) -> Callable[[Optional[T]], Optional[R]]:
    def transform_column(value: Optional[T]) -> Optional[R]:
        if value is None:
            return value
        else:
            return function(value)

    return transform_column


def build_result_transformer(
    column_transformations: Sequence[Tuple[Pattern[str], Callable[[Any], Any]]],
) -> Callable[[Result], None]:
    """
    Builds and returns a function that can be used to mutate a ``Result``
    instance in-place by transforming all values for columns that have a
    transformation function specified for their data type.
    """

    def transform_result(result: Result) -> None:
        for column in result["meta"]:
            is_nullable, type = unwrap_nullable_type(column["type"])

            transformer = next(
                (
                    transformer
                    for pattern, transformer in column_transformations
                    if pattern.match(type)
                ),
                None,
            )

            if transformer is None:
                continue

            if is_nullable:
                transformer = transform_nullable(transformer)

            name = column["name"]
            for row in iterate_rows(result):
                row[name] = transformer(row[name])

    return transform_result


class Reader(ABC):
    def __init__(
        self, cache_partition_id: Optional[str], query_settings_prefix: Optional[str]
    ) -> None:
        self.__cache_partition_id = cache_partition_id
        self.__query_settings_prefix = query_settings_prefix

    @abstractmethod
    def execute(
        self,
        query: FormattedQuery,
        settings: Optional[Mapping[str, str]] = None,
        with_totals: bool = False,
        robust: bool = False,
        capture_trace: bool = False,
    ) -> Result:
        """Execute a query."""
        raise NotImplementedError

    @property
    def cache_partition_id(self) -> Optional[str]:
        """
        Return the cache partition if there is one.

        TODO: If we double down on having the cache at Clickhouse query level
        we should move the entire caching infrastructure either here or in
        the cluster.
        If we, instead, move the cache towards the logical level, all this
        should go away.
        """
        return self.__cache_partition_id

    def get_query_settings_prefix(self) -> Optional[str]:
        """
        Return the query settings prefix if there is one.
        """
        return self.__query_settings_prefix
