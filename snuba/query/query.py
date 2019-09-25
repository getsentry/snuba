from __future__ import annotations

from deprecation import deprecated
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)
from dataclasses import dataclass


Condition = Union[
    Tuple[Any, Any, Any],
    Sequence[Any],
]

Aggregation = Union[
    Tuple[Any, Any, Any],
    Sequence[Any],
]

Groupby = Sequence[Any]

TElement = TypeVar("TElement")


class Query:
    """
    Represents a parsed query we can edit during query processing.

    This is the bare minimum abstraction to avoid depending on a mutable
    Mapping around the code base. Fully untagling the query representation
    from the code depnding on it wil ltake a lot of PRs, but at least we
    have a basic abstraction to move functionalities to.
    It is also the base to split the Clickhouse specific query into
    an abstract Snuba query and a concrete Clickhouse query, but
    that cannot come in this PR since it also requires a proper
    schema split in the dataset to happen.
    """
    # TODO: Make getters non nullable when possible. This is a risky
    # change so we should take one field at a time.

    def __init__(self, body: MutableMapping[str, Any]):
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__body = body

    def __extend_sequence(self,
        field: str,
        content: Sequence[TElement],
    ) -> None:
        if field not in self.__body:
            self.__body[field] = []
        self.__body[field].extend(content)

    def get_selected_columns(self) -> Optional[Sequence[Any]]:
        return self.__body.get("selected_columns")

    def set_selected_columns(
        self,
        columns: Sequence[Any],
    ) -> None:
        self.__body["selected_columns"] = columns

    def get_aggregations(self) -> Optional[Sequence[Aggregation]]:
        return self.__body.get("aggregations")

    def set_aggregations(
        self,
        aggregations: Sequence[Aggregation],
    ) -> None:
        self.__body["aggregations"] = aggregations

    def get_groupby(self) -> Optional[Sequence[Groupby]]:
        return self.__body.get("groupby")

    def add_groupby(
        self,
        groupby: Sequence[Groupby],
    ) -> None:
        self.__extend_sequence("groupby", groupby)

    def get_conditions(self) -> Optional[Sequence[Condition]]:
        return self.__body.get("conditions")

    def set_conditions(
        self,
        conditions: Sequence[Condition]
    ) -> None:
        self.__body["conditions"] = conditions

    def add_conditions(
        self,
        conditions: Sequence[Condition],
    ) -> None:
        self.__extend_sequence("conditions", conditions)

    def get_orderby(self) -> Optional[Sequence[Any]]:
        return self.__body.get("orderby")

    def get_sample(self) -> Optional[float]:
        return self.__body.get("sample")

    def set_sample(self, sample: float) -> None:
        self.__body["sample"] = sample

    def get_limit(self) -> int:
        return self.__body.get('limit', 0)

    def set_limit(self, limit: int) -> None:
        self.__body["limit"] = limit

    def get_offset(self) -> int:
        return self.__body.get('offset', 0)

    def set_offset(self, offset: int) -> None:
        self.__body["offset"] = offset

    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods instead.")
    def get_body(self) -> Mapping[str, Any]:
        return self.__body


@dataclass
class QueryHints:
    turbo: bool
    final: bool
