from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from itertools import chain
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from deprecation import deprecated

from snuba.datasets.schemas import RelationalSource
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.expressions import (
    Column,
    Expression,
    ExpressionVisitor,
    SubscriptableReference,
)
from snuba.query.types import Condition

Aggregation = Union[
    Tuple[Any, Any, Any], Sequence[Any],
]

Limitby = Tuple[int, str]

TElement = TypeVar("TElement")

TExp = TypeVar("TExp", bound=Expression)


class OrderByDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True)
class OrderBy:
    direction: OrderByDirection
    expression: Expression


@dataclass(frozen=True)
class SelectedExpression:
    # The name of this column in the resultset.
    name: Optional[str]
    expression: Expression


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

    NEW DATA MODEL:
    The query is represented as a tree. The Query object is the root.
    Nodes in the tree can have different types (Expression, Conditions, etc.)
    Each node could be an individual node (like a column) or an collection.
    A collection can be a sequence (like a list of Columns) or a hierarchy
    (like function calls).

    There are three ways to manipulate the query:
    - traverse the tree. Traversing the full tree in an untyped way is
      not extremely useful. What is more interesting is being able to
      iterate over all the nodes of a given type (like expressions).
      This is achieved through the NodeContainer interface.

    - replace specific nodes. NodeContainer provides a map methods that
      allows the callsite to apply a func to all nodes of a specific
      type. This is useful for replacing expressions across the query.

    - direct access to the root and explore specific parts of the tree
      from there.
    """

    # TODO: Make getters non nullable when possible. This is a risky
    # change so we should take one field at a time.

    def __init__(
        self,
        body: MutableMapping[str, Any],  # Temporary
        data_source: Optional[RelationalSource],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Expression] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        entity_name: Optional[str] = None,
    ):
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__body = body
        self.__final = False
        self.__data_source = data_source

        self.__selected_columns = selected_columns or []
        self.__array_join = array_join
        self.__condition = condition
        self.__prewhere = prewhere
        self.__groupby = groupby or []
        self.__having = having
        self.__order_by = order_by or []
        self.__entity_name = entity_name

    def get_all_expressions(self) -> Iterable[Expression]:
        """
        Returns an expression container that iterates over all the expressions
        in the query no matter which level of nesting they are at.
        The ExpressionContainer can be used to traverse the expressions in the
        tree.
        """
        return chain(
            chain.from_iterable(
                map(lambda selected: selected.expression, self.__selected_columns)
            ),
            self.__array_join or [],
            self.__condition or [],
            chain.from_iterable(self.__groupby),
            self.__having or [],
            chain.from_iterable(
                map(lambda orderby: orderby.expression, self.__order_by)
            ),
        )

    def transform_expressions(self, func: Callable[[Expression], Expression],) -> None:
        """
        Transforms in place the current query object by applying a transformation
        function to all expressions contained in this query

        Contrary to Expression.transform, this happens in place since Query has
        to be mutable as of now. This is because there are still parts of the query
        processing that depends on the Query instance not to be replaced during the
        query. See the Request class (that is immutable, so Query cannot be replaced).
        """

        def transform_expression_list(
            expressions: Sequence[Expression],
        ) -> Sequence[Expression]:
            return list(map(lambda exp: exp.transform(func), expressions),)

        self.__selected_columns = list(
            map(
                lambda selected: replace(
                    selected, expression=selected.expression.transform(func)
                ),
                self.__selected_columns,
            )
        )
        self.__array_join = (
            self.__array_join.transform(func) if self.__array_join else None
        )
        self.__condition = (
            self.__condition.transform(func) if self.__condition else None
        )
        self.__groupby = transform_expression_list(self.__groupby)
        self.__having = self.__having.transform(func) if self.__having else None
        self.__order_by = list(
            map(
                lambda clause: replace(
                    clause, expression=clause.expression.transform(func)
                ),
                self.__order_by,
            )
        )

    def transform(self, visitor: ExpressionVisitor[Expression]) -> None:
        """
        Applies a transformation, defined through a Visitor, to the
        entire query. Here the visitor is supposed to return a new
        Expression and it is applied to each root Expression in this
        query, where a root Expression is an Expression that does not
        have another Expression as parent.
        The transformation happens in place.
        """

        self.__selected_columns = list(
            map(
                lambda selected: replace(
                    selected, expression=selected.expression.accept(visitor)
                ),
                self.__selected_columns,
            )
        )
        if self.__array_join is not None:
            self.__array_join = self.__array_join.accept(visitor)
        if self.__condition is not None:
            self.__condition = self.__condition.accept(visitor)
        if self.__prewhere is not None:
            self.__prewhere = self.__prewhere.accept(visitor)
        self.__groupby = [e.accept(visitor) for e in (self.__groupby or [])]
        if self.__having is not None:
            self.__having = self.__having.accept(visitor)
        self.__order_by = list(
            map(
                lambda clause: replace(
                    clause, expression=clause.expression.accept(visitor)
                ),
                self.__order_by,
            )
        )

    def get_data_source(self) -> RelationalSource:
        """
        After the split between dataset query class and storage query class
        this method will only be in the storage query class, which will be instantiated
        with a non nullable RelationalSource.
        """
        assert self.__data_source is not None, "Data source has not been provided yet."
        return self.__data_source

    def set_data_source(self, data_source: RelationalSource) -> None:
        self.__data_source = data_source

    def get_selected_columns_from_ast(self) -> Sequence[SelectedExpression]:
        return self.__selected_columns

    def set_ast_selected_columns(
        self, selected_columns: Sequence[SelectedExpression]
    ) -> None:
        self.__selected_columns = selected_columns

    def get_groupby_from_ast(self) -> Sequence[Expression]:
        return self.__groupby

    def get_conditions(self) -> Optional[Sequence[Condition]]:
        return self.__body.get("conditions")

    def get_condition_from_ast(self) -> Optional[Expression]:
        return self.__condition

    def set_ast_condition(self, condition: Optional[Expression]) -> None:
        self.__condition = condition

    def set_conditions(self, conditions: Sequence[Condition]) -> None:
        self.__body["conditions"] = conditions

    def add_condition_to_ast(self, condition: Expression) -> None:
        if not self.__condition:
            self.__condition = condition
        else:
            self.__condition = binary_condition(
                None, BooleanFunctions.AND, condition, self.__condition
            )

    def get_prewhere_ast(self) -> Optional[Expression]:
        """
        Temporary method until pre where management is moved to Clickhouse query
        """
        return self.__prewhere

    def set_prewhere_ast_condition(self, condition: Optional[Expression]) -> None:
        self.__prewhere = condition

    def get_arrayjoin_from_ast(self) -> Optional[Expression]:
        return self.__array_join

    def get_having_from_ast(self) -> Optional[Expression]:
        return self.__having

    def set_ast_having(self, condition: Optional[Expression]) -> None:
        self.__having = condition

    def get_orderby_from_ast(self) -> Sequence[OrderBy]:
        return self.__order_by

    def get_limitby(self) -> Optional[Limitby]:
        return self.__body.get("limitby")

    def get_sample(self) -> Optional[float]:
        return self.__body.get("sample")

    def get_limit(self) -> Optional[int]:
        return self.__body.get("limit", None)

    def set_limit(self, limit: int) -> None:
        self.__body["limit"] = limit

    def get_offset(self) -> int:
        return self.__body.get("offset", 0)

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
        "use the specific accessor methods instead."
    )
    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def __get_all_ast_referenced_expressions(
        self, expressions: Iterable[Expression], exp_type: Type[TExp]
    ) -> Set[TExp]:
        ret: Set[TExp] = set()
        for expression in expressions:
            ret |= {c for c in expression if isinstance(c, exp_type)}
        return ret

    def get_all_ast_referenced_columns(self) -> Set[Column]:
        return self.__get_all_ast_referenced_expressions(
            self.get_all_expressions(), Column
        )

    def get_all_ast_referenced_subscripts(self) -> Set[SubscriptableReference]:
        return self.__get_all_ast_referenced_expressions(
            self.get_all_expressions(), SubscriptableReference
        )

    def get_columns_referenced_in_conditions_ast(self) -> Set[Column]:
        return self.__get_all_ast_referenced_expressions(
            [self.__condition] if self.__condition is not None else [], Column
        )

    def validate_aliases(self) -> bool:
        """
        Returns true if all the alias reference in this query can be resolved.

        Which means, they are either declared somewhere in the query itself
        or they are referencing columns in the table.

        Caution: for this to work, data_source needs to be already populated,
        otherwise it would throw.
        """
        declared_symbols: Set[str] = set()
        referenced_symbols: Set[str] = set()
        for e in self.get_all_expressions():
            # SELECT f(g(x)) as A -> declared_symbols = {A}
            # SELECT a as B -> declared_symbols = {B} referenced_symbols = {a}
            # SELECT a AS a -> referenced_symbols = {a}
            if e.alias:
                if isinstance(e, Column):
                    qualified_col_name = (
                        e.column_name
                        if not e.table_name
                        else f"{e.table_name}.{e.column_name}"
                    )
                    referenced_symbols.add(qualified_col_name)
                    if e.alias != qualified_col_name:
                        declared_symbols.add(e.alias)
                else:
                    declared_symbols.add(e.alias)
            else:
                if isinstance(e, Column) and not e.alias and not e.table_name:
                    referenced_symbols.add(e.column_name)

        declared_symbols |= {c.flattened for c in self.get_data_source().get_columns()}
        return not referenced_symbols - declared_symbols

    def set_entity_name(self, entity_name: str) -> None:
        assert self.__entity_name is None
        self.__entity_name = entity_name

    def get_entity_name(self) -> str:
        assert self.__entity_name is not None
        return self.__entity_name
