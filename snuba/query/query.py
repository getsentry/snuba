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
    SAFE_COL_RE,
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
        self.__prewhere_conditions: Sequence[Condition] = []

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

    def set_groupby(
        self,
        groupby: Sequence[Aggregation],
    ) -> None:
        self.__body["groupby"] = groupby

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

    def get_prewhere(self) -> Sequence[Condition]:
        """
        Temporary method until pre where management is moved to Clickhouse query
        """
        return self.__prewhere_conditions

    def set_prewhere(
        self,
        conditions: Sequence[Condition]
    ) -> None:
        """
        Temporary method until pre where management is moved to Clickhouse query
        """
        self.__prewhere_conditions = conditions

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

    def set_orderby(
        self,
        orderby: Sequence[Any]
    ) -> None:
        self.__body["orderby"] = orderby

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

    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods instead.")
    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def get_all_referenced_columns(self) -> Sequence[Any]:
        """
        Return the set of all columns that are used by a query.

        TODO: This does not actually return all columns referenced in the query since
        there are some corner cases left out:
        - functions expressed in the form f(column) in aggregations.

        Will fix both when adding a better column abstraction.
        Also the replace_column method behave consistently with this one. Any change to
        this method should be reflected there.
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
        self.__add_flat_conditions(col_exprs, self.get_conditions())
        self.__add_flat_conditions(col_exprs, self.get_having())
        self.__add_flat_conditions(col_exprs, self.get_prewhere())

        if self.get_aggregations():
            col_exprs.extend([a[1] for a in self.get_aggregations()])

        # Return the set of all columns referenced in any expression
        return self.__get_referenced_columns(col_exprs)

    def get_columns_referenced_in_conditions(self) -> Sequence[Any]:
        col_exprs: MutableSequence[Any] = []
        self.__add_flat_conditions(col_exprs, self.get_conditions())
        return self.__get_referenced_columns(col_exprs)

    def get_columns_referenced_in_having(self) -> Sequence[Any]:
        col_exprs: MutableSequence[Any] = []
        self.__add_flat_conditions(col_exprs, self.get_having())
        return self.__get_referenced_columns(col_exprs)

    def __add_flat_conditions(self, col_exprs: MutableSequence[Any], conditions=Optional[Sequence[Condition]]) -> None:
        if conditions:
            flat_conditions = list(chain(*[[c] if is_condition(c) else c for c in conditions]))
            col_exprs.extend([c[0] for c in flat_conditions])

    def __get_referenced_columns(self, col_exprs: MutableSequence[Any]) -> Sequence[Any]:
        return set(chain(*[columns_in_expr(ex) for ex in col_exprs]))

    def __replace_col_in_expression(
        self,
        expression: Any,
        old_column: str,
        new_column: str,
    ) -> Any:
        """
        Returns a copy of the expression with old_column replaced by new_column.
        This does not modify anything in place since expressions can be either
        mutable (lists) or immutable (strings and tuples), so the only way to be
        consistent in the result is always to return a brand new expression.

        This does not support all possible columns format (like string representations
        of function expresison "count(col1)"). But it is consistent with the behavior
        of get_all_referenced_columns in that it won't replace something that is not
        returned by that function and it will replace all the columns returned by that
        function.
        """
        if isinstance(expression, str):
            match = SAFE_COL_RE.match(expression)
            if match and match[1] == old_column:
                return expression.replace(old_column, new_column)
        elif (isinstance(expression, (list, tuple)) and len(expression) >= 2
                and isinstance(expression[1], (list, tuple))):
            params = [
                self.__replace_col_in_expression(param, old_column, new_column)
                for param in expression[1]
            ]
            ret = [expression[0], params]
            if len(expression) == 3:
                # Alias for the column
                ret.append(expression[2])
            return ret
        return expression

    def __replace_col_in_condition(
        self,
        condition: Condition,
        old_column: str,
        new_column: str,
    ) -> Condition:
        """
        Replaces a column in a structured condition. This is a level above replace_col_in_expression
        since conditions are in the form [expression, operator, literal] (which is not fully correct
        since the right side of the condition should be an expression as well but this constraint is
        imposed by the query schema and get_all_referenced_columns behaves accordingly).
        Conditions can also be nested.
        """
        if is_condition(condition):
            return [
                self.__replace_col_in_expression(condition[0], old_column, new_column),
                condition[1],
                condition[2]
            ]
        elif isinstance(condition, (tuple, list)):
            # nested condition
            return [
                self.__replace_col_in_condition(cond, old_column, new_column) for cond in condition
            ]
        else:
            # Don't know what this is
            return condition

    def __replace_col_in_list(
        self,
        expressions: Any,
        old_column: str,
        new_column: str,
    ) -> Sequence[Any]:
        return [
            self.__replace_col_in_expression(expr, old_column, new_column)
            for expr in to_list(expressions)
        ]

    def replace_column(self, old_column: str, new_column: str) -> None:
        """
        Replaces a column in all fields of the query. The Query object is mutated in place
        while the internal fields are replaced.

        This behaves consistently with get_all_referenced_columns (which does not really
        behave correctly since it is missing a few fields that can contain columns). Will
        fix both when adding a better column abstraction.

        In the current implementation we can only replace a column identified by a string
        with another column identified by a string. This does not support replacing a
        column with a complex expression.
        Columns represented as strings include expresions like "tags[a]" or "f(column)"
        This method will replace them as well if requested, but that would not be a good
        idea since such columns are processed by column_expr later in the flow.
        """

        if self.get_selected_columns():
            self.set_selected_columns(self.__replace_col_in_list(
                self.get_selected_columns(),
                old_column,
                new_column,
            ))

        if self.get_arrayjoin():
            self.set_arrayjoin(
                self.__replace_col_in_expression(self.get_arrayjoin(), old_column, new_column)
            )

        if self.get_groupby():
            self.set_groupby(self.__replace_col_in_list(
                self.get_groupby(),
                old_column,
                new_column,
            ))

        if self.get_orderby():
            self.set_orderby(self.__replace_col_in_list(
                self.get_orderby(),
                old_column,
                new_column,
            ))

        if self.get_aggregations():
            self.set_aggregations([
                [
                    aggr[0],
                    self.__replace_col_in_expression(aggr[1], old_column, new_column)
                    if not isinstance(aggr[1], (list, tuple))
                    # This can be an expresison or a list of expressions
                    else self.__replace_col_in_list(aggr[1], old_column, new_column),
                    aggr[2],
                ] for aggr in to_list(self.get_aggregations())
            ])

        if self.get_conditions():
            self.set_conditions(
                self.__replace_col_in_condition(
                    to_list(self.get_conditions()),
                    old_column,
                    new_column,
                )
            )
