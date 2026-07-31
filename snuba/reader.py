from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from re import Pattern
from typing import (
    Any,
    TypedDict,
    TypeVar,
)

from snuba.clickhouse.formatter.nodes import FormattedQuery


class Column(TypedDict, total=False):
    name: str
    type: str


Row = dict[str, Any]


class Result(TypedDict, total=False):
    meta: list[Column]
    data: list[Row]
    totals: Row
    profile: dict[str, Any] | None
    trace_output: str


def iterate_rows(result: Result) -> Iterator[Row]:
    if "totals" in result:
        return itertools.chain(result["data"], [result["totals"]])
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


def unwrap_nullable_type(type: str) -> tuple[bool, str]:
    match = NULLABLE_RE.match(type)
    if match is not None:
        return True, match.groups()[0]
    return False, type


T = TypeVar("T")
R = TypeVar("R")


def transform_nullable(function: Callable[[T], R]) -> Callable[[T | None], R | None]:
    def transform_column(value: T | None) -> R | None:
        if value is None:
            return value
        return function(value)

    return transform_column


def build_result_transformer(
    column_transformations: Sequence[tuple[Pattern[str], Callable[[Any], Any]]],
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
    def __init__(self, cache_partition_id: str | None, query_settings_prefix: str | None) -> None:
        self.__cache_partition_id = cache_partition_id
        self.__query_settings_prefix = query_settings_prefix

    @abstractmethod
    def execute(
        self,
        query: FormattedQuery,
        settings: Mapping[str, str] | None = None,
        with_totals: bool = False,
        robust: bool = False,
        capture_trace: bool = False,
    ) -> Result:
        """Execute a query."""
        raise NotImplementedError

    @property
    def cache_partition_id(self) -> str | None:
        """
        Return the cache partition if there is one.

        TODO: If we double down on having the cache at Clickhouse query level
        we should move the entire caching infrastructure either here or in
        the cluster.
        If we, instead, move the cache towards the logical level, all this
        should go away.
        """
        return self.__cache_partition_id

    def get_query_settings_prefix(self) -> str | None:
        """
        Return the query settings prefix if there is one.
        """
        return self.__query_settings_prefix
