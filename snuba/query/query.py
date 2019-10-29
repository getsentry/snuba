from __future__ import annotations

from deprecation import deprecated
from itertools import chain
from typing import (
    Any,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from snuba.datasets.schemas import RelationalSource
from snuba.query.types import Condition
from snuba.util import (
    columns_in_expr,
    is_condition,
    to_list
)

Aggregation = Union[
    Tuple[Any, Any, Any],
    Sequence[Any],
]

Groupby = Sequence[Any]

Limitby = Tuple[int, str]

TElement = TypeVar("TElement")


class Query:
    """
    Represents a parsed query we can edit during query processing.

    This is the bare minimum abstraction to avoid depending on a mutable
    Mapping around the code base. Fully untangling the query representation
    from the code depending on it wil take a lot of PRs, but at least we
    have a basic abstraction to move functionalities to.
    It is also the base to split the Clickhouse specific query into
    an abstract Snuba query and a concrete Clickhouse query, but
    that cannot come in this PR since it also requires a proper
    schema split in the dataset to happen.
    """
    # TODO: Make getters non nullable when possible. This is a risky
    # change so we should take one field at a time.

    def __init__(self, body: MutableMapping[str, Any], data_source: RelationalSource):
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__body = body
        self.__final = False
        self.__data_source = data_source
        self.__with = None

    def get_data_source(self) -> RelationalSource:
        return self.__data_source

    def set_data_source(self, data_source: RelationalSource) -> None:
        self.__data_source = data_source

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

    def set_arrayjoin(
        self,
        arrayjoin: str
    ) -> None:
        self.__body["arrayjoin"] = arrayjoin

    def get_arrayjoin(self) -> Optional[str]:
        return self.__body.get("arrayjoin", None)

    def get_having(self) -> Sequence[Condition]:
        return self.__body.get("having", [])

    def get_orderby(self) -> Optional[Sequence[Any]]:
        return self.__body.get("orderby")

    def get_limitby(self) -> Optional[Limitby]:
        return self.__body.get("limitby")

    def get_sample(self) -> Optional[float]:
        return self.__body.get("sample")

    def get_limit(self) -> Optional[int]:
        return self.__body.get('limit', None)

    def set_limit(self, limit: int) -> None:
        self.__body["limit"] = limit

    def get_offset(self) -> int:
        return self.__body.get('offset', 0)

    def set_offset(self, offset: int) -> None:
        self.__body["offset"] = offset

    def has_totals(self) -> bool:
        return self.__body.get("totals", False)

    def get_final(self) -> bool:
        return self.__final

    def set_final(self, final: bool) -> None:
        self.__final = final

    def set_granularity(self, granularity: int) -> None:
        """
        Temporary method to move the management of granularity
        out of the body. This is the only usage left of the body
        during the query column processing. That processing is
        going to move to a place where this object is being built.
        But in order to do that we need first to decouple it from
        the query body, thus we need to move the granularity out of
        the body even if it does not really belong here.
        """
        self.__body["granularity"] = granularity

    def get_granularity(self) -> Optional[int]:
        return self.__body.get("granularity")

    def get_with(self) -> Sequence[Tuple[str, str]]:
        return self.__with

    def set_with(self, _with: Sequence[Tuple[str, str]]) -> None:
        self.__with = _with

    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods instead.")
    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def get_all_referenced_columns(self) -> Sequence[Any]:
        """
        Return the set of all columns that are used by a query.
        """
        col_exprs: MutableSequence[Any] = []

        if self.get_arrayjoin():
            col_exprs.extend(to_list(self.get_arrayjoin()))
        if self.get_groupby():
            col_exprs.extend(to_list(self.get_groupby()))
        if self.get_orderby():
            col_exprs.extend(to_list(self.get_orderby()))
        if self.get_selected_columns():
            col_exprs.extend(to_list(self.get_selected_columns()))

        # Conditions need flattening as they can be nested as AND/OR
        self.__add_flat_conditions(col_exprs)
        if self.get_conditions():
            flat_conditions = list(chain(*[[c] if is_condition(c) else c for c in self.get_conditions()]))
            col_exprs.extend([c[0] for c in flat_conditions])

        if self.get_aggregations():
            col_exprs.extend([a[1] for a in self.get_aggregations()])

        # Return the set of all columns referenced in any expression
        return self.__get_referenced_columns(col_exprs)

    def get_columns_referenced_in_conditions(self) -> Sequence[Any]:
        col_exprs: MutableSequence[Any] = []
        self.__add_flat_conditions(col_exprs)
        return self.__get_referenced_columns(col_exprs)

    def __add_flat_conditions(self, col_exprs: MutableSequence[Any]):
        if self.get_conditions():
            flat_conditions = list(chain(*[[c] if is_condition(c) else c for c in self.get_conditions()]))
            col_exprs.extend([c[0] for c in flat_conditions])

    def __get_referenced_columns(self, col_exprs: MutableSequence[Any]):
        return set(chain(*[columns_in_expr(ex) for ex in col_exprs]))
