from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum
from itertools import chain
from typing import Any as AnyType
from typing import (
    Callable,
    Generic,
    Iterable,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from snuba.clickhouse.columns import Any, ColumnSet
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source import DataSource
from snuba.query.data_source.simple import SimpleDataSource
from snuba.query.expressions import (
    Column,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    SubscriptableReference,
)


class FromClauseNotSet(Exception):
    pass


@dataclass(frozen=True)
class LimitBy:
    limit: int
    columns: Sequence[Expression]


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
    # TODO: Make this non nullable
    name: Optional[str]
    expression: Expression


TExp = TypeVar("TExp", bound=Expression)


class Query(DataSource, ABC):
    """
    The representation of a query in Snuba.
    This class can be extended to support either the logical query which
    is based on a concept of graph or a physical query which is
    relational or any nested structure.

    It provides the types of nodes that we expect to be present for
    all types of queries and high level methods to manipulate them.
    Each of the node is an AST expression or a sequence of AST
    expressions.

    This class can represent either a query on a simple data source
    (entity or relational source) or a composite query with nested
    queries, joins, or a combination of them.

    There are three ways to manipulate the query:
    - direct access methods to individual nodes.
    - iterate and transform over all the expressions. This is useful
      for simple transformations since the transformation function
      used has no context of where the expression is in the query.
    - provide a visitor that is executed on all expression trees.
      With this approach, the visitor has full context on each expression
      tree that is transformed.
    """

    def __init__(
        self,
        # TODO: Consider if to remove the defaults and make some of
        # these fields mandatory. This impacts a lot of code so it
        # would be done on its own.
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
        experiments: Optional[MutableMapping[str, AnyType]] = None,
    ):
        self.__selected_columns = selected_columns or []
        self.__array_join = array_join
        self.__condition = condition
        self.__groupby = groupby or []
        self.__having = having
        self.__order_by = order_by or []
        self.__limitby = limitby
        self.__limit = limit
        self.__offset = offset
        self.__totals = totals
        self.__granularity = granularity
        self.__experiments = experiments or {}

    def get_columns(self) -> ColumnSet:
        """
        From the DataSource class. It returns the schema exposed by this
        query when used as a Data Source for another query.
        """
        ret = []
        for index, selected_col in enumerate(self.__selected_columns):
            name = selected_col.name
            # TODO: Make the type of the columns precise onn the type
            # when possible. It may become useful for query validation
            # but it would be best effort because we cannot infer the
            # type of complex expressions.
            ret.append(
                (name, Any())
                if name is not None
                # This should never happen for nested queries.
                # Though the type of the name oof a selected column is
                # still optional soo we need to fix that first.
                else (f"_invalid_alias_{index}", Any())
            )

        return ColumnSet(ret)

    @abstractmethod
    def get_from_clause(self) -> DataSource:
        raise NotImplementedError

    # TODO: Run a codemod to remove the "from_ast" from all these
    # methods.
    def get_selected_columns(self) -> Sequence[SelectedExpression]:
        return self.__selected_columns

    def set_ast_selected_columns(
        self, selected_columns: Sequence[SelectedExpression]
    ) -> None:
        self.__selected_columns = selected_columns

    def get_groupby(self) -> Sequence[Expression]:
        return self.__groupby

    def set_ast_groupby(self, groupby: Sequence[Expression]) -> None:
        self.__groupby = groupby

    def get_condition(self) -> Optional[Expression]:
        return self.__condition

    def set_ast_condition(self, condition: Optional[Expression]) -> None:
        self.__condition = condition

    def add_condition_to_ast(self, condition: Expression) -> None:
        if not self.__condition:
            self.__condition = condition
        else:
            self.__condition = binary_condition(
                BooleanFunctions.AND, condition, self.__condition
            )

    def get_arrayjoin(self) -> Optional[Sequence[Expression]]:
        return self.__array_join

    def set_arrayjoin(self, arrayjoin: Optional[Sequence[Expression]]) -> None:
        self.__array_join = arrayjoin

    def get_having(self) -> Optional[Expression]:
        return self.__having

    def set_ast_having(self, condition: Optional[Expression]) -> None:
        self.__having = condition

    def get_orderby(self) -> Sequence[OrderBy]:
        return self.__order_by

    def set_ast_orderby(self, orderby: Sequence[OrderBy]) -> None:
        self.__order_by = orderby

    def get_limitby(self) -> Optional[LimitBy]:
        return self.__limitby

    def set_limitby(self, limitby: LimitBy) -> None:
        self.__limitby = limitby

    def get_limit(self) -> Optional[int]:
        return self.__limit

    def set_limit(self, limit: int) -> None:
        self.__limit = limit

    def get_offset(self) -> int:
        return self.__offset

    def set_offset(self, offset: int) -> None:
        self.__offset = offset

    def set_totals(self, totals: bool) -> None:
        self.__totals = totals

    def has_totals(self) -> bool:
        return self.__totals

    def set_granularity(self, granularity: int) -> None:
        self.__granularity = granularity

    def get_granularity(self) -> Optional[int]:
        return self.__granularity

    def add_experiment(self, name: str, value: AnyType) -> None:
        self.__experiments[name] = value

    def set_experiments(self, experiments: MutableMapping[str, AnyType]) -> None:
        self.__experiments = experiments

    def get_experiments(self) -> MutableMapping[str, AnyType]:
        return self.__experiments

    def get_experiment_value(self, name: str) -> AnyType:
        return self.__experiments.get(name)

    @abstractmethod
    def _get_expressions_impl(self) -> Iterable[Expression]:
        """
        Provides an iterable on all additional nodes added by the children
        on top of this query structure.
        """
        raise NotImplementedError

    def get_all_expressions(self) -> Iterable[Expression]:
        """
        Traverses the entire query tree and returns every expression
        found.
        No guarantee around the order is provided, and it does not
        deduplicate any of the expressions found.
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
            self.__limitby.columns if self.__limitby else [],
            self._get_expressions_impl(),
        )

    @abstractmethod
    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        """
        Applies the transformation function to all the nodes added to the
        query by the children of this class.
        Transformation happens in place.
        See the `transform_expressions` method for details on how this is
        applied.
        """
        raise NotImplementedError

    def transform_expressions(
        self,
        func: Callable[[Expression], Expression],
        skip_transform_condition: bool = False,
        skip_array_join: bool = False,
    ) -> None:
        """
        Transforms in place the current query object by applying a transformation
        function to all expressions contained in this query.

        Contrary to Expression.transform, this happens in place since Query has
        to be mutable as of now. This is because there are still parts of the query
        processing that depends on the Query instance not to be replaced during the
        query.
        """

        def transform_expression_list(
            expressions: Sequence[Expression],
        ) -> Sequence[Expression]:
            return list(
                map(lambda exp: exp.transform(func), expressions),
            )

        self.__selected_columns = list(
            map(
                lambda selected: replace(
                    selected, expression=selected.expression.transform(func)
                ),
                self.__selected_columns,
            )
        )
        if not skip_array_join:
            if self.__array_join:
                self.__array_join = [
                    join_element.transform(func) for join_element in self.__array_join
                ]

        if not skip_transform_condition:
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

        if self.__limitby is not None:
            self.__limitby = LimitBy(
                self.__limitby.limit,
                [column.transform(func) for column in self.__limitby.columns],
            )

        self._transform_expressions_impl(func)

    @abstractmethod
    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        """
        Applies a transformation, defined through a Visitor to the
        nodes added by any child class.
        See the `transform` method for how this is applied.
        """
        raise NotImplementedError

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
            self.__array_join = [
                join_element.accept(visitor) for join_element in self.__array_join
            ]
        if self.__condition is not None:
            self.__condition = self.__condition.accept(visitor)
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
        if self.__limitby is not None:
            self.__limitby = LimitBy(
                self.__limitby.limit,
                [expression.accept(visitor) for expression in self.__limitby.columns],
            )
        self._transform_impl(visitor)

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

    def get_columns_referenced_in_select(self) -> Set[Column]:
        return self.__get_all_ast_referenced_expressions(
            [selected.expression for selected in self.__selected_columns], Column
        )

    def find_and_replace_column_in_condition(
        self, find_column_name: str, replace_column: Column
    ) -> None:
        condition = self.get_condition()
        new_conditions = self._rebuild_condition_tree_with_replaced_column(
            condition, find_column_name, replace_column
        )
        self.set_ast_condition(new_conditions)

    def _rebuild_condition_tree_with_replaced_column(
        self,
        condition: Optional[Expression],
        find_column_name: str,
        replace_column: Column,
    ) -> Expression:
        assert isinstance(condition, FunctionCall)
        parameters = []
        for parameter in condition.parameters:
            if isinstance(parameter, FunctionCall):
                new_parameter = self._rebuild_condition_tree_with_replaced_column(
                    parameter, find_column_name, replace_column
                )
                parameters.append(new_parameter)
            else:
                if isinstance(parameter, Column):
                    if parameter.column_name == find_column_name:
                        parameter = replace_column
                parameters.append(parameter)

        return FunctionCall(None, condition.function_name, tuple(parameters))

    def find_and_replace_column_in_groupby_and_selected_columns(
        self, find_column_name: str, replace_column: Column
    ) -> None:
        groupby = self.get_groupby()
        new_groupby_columns = []
        for column in groupby:
            assert isinstance(column, Column)
            if column.column_name == find_column_name:
                column = replace_column
            new_groupby_columns.append(column)

        selected_columns = self.get_selected_columns()
        new_selected_columns = []
        for selected_column in selected_columns:
            assert isinstance(selected_column, SelectedExpression)
            if (
                isinstance(selected_column.expression, Column)
                and selected_column.expression.column_name == find_column_name
            ):
                selected_column = SelectedExpression(
                    selected_column.name, replace_column
                )
            new_selected_columns.append(selected_column)

        self.set_ast_groupby(new_groupby_columns)
        self.set_ast_selected_columns(new_selected_columns)

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

        declared_symbols |= {c.flattened for c in self.get_from_clause().get_columns()}
        return not referenced_symbols - declared_symbols

    def _eq_functions(self) -> Sequence[str]:
        return (
            "get_selected_columns",
            "get_groupby",
            "get_condition",
            "get_arrayjoin",
            "get_having",
            "get_orderby",
            "get_limitby",
            "get_limit",
            "get_offset",
            "has_totals",
            "get_granularity",
        )

    def equals(self, other: object) -> Tuple[bool, str]:
        if self.__class__ != other.__class__:
            return False, f"{self.__class__} != {other.__class__}"

        tests = self._eq_functions()
        for func in tests:
            if getattr(self, func)() != getattr(other, func)():
                return (
                    False,
                    f"{func}: {getattr(self, func)()} != {getattr(other, func)()}",
                )
        return True, ""

    def __eq__(self, other: object) -> bool:
        eq, _ = self.equals(other)
        return eq


TSimpleDataSource = TypeVar("TSimpleDataSource", bound=SimpleDataSource)


class ProcessableQuery(Query, ABC, Generic[TSimpleDataSource]):
    """
    A Query class that can be used by query processors and translators.
    Specifically its data source can only be a SimpleDataSource.
    """

    def __init__(
        self,
        from_clause: Optional[TSimpleDataSource],
        # TODO: Consider if to remove the defaults and make some of
        # these fields mandatory. This impacts a lot of code so it
        # would be done on its own.
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
    ):
        super().__init__(
            selected_columns=selected_columns,
            array_join=array_join,
            condition=condition,
            groupby=groupby,
            having=having,
            order_by=order_by,
            limitby=limitby,
            limit=limit,
            offset=offset,
            totals=totals,
            granularity=granularity,
        )
        self.__from_clause = from_clause

    def get_from_clause(self) -> TSimpleDataSource:
        if self.__from_clause is None:
            raise FromClauseNotSet("Data source has not been provided yet.")
        return self.__from_clause

    def set_from_clause(self, from_clause: TSimpleDataSource) -> None:
        self.__from_clause = from_clause

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_from_clause",)

    def __repr__(self) -> str:
        from snuba.query.formatters.tracing import format_query

        # NOTE (Vlad): Why the type is cast:
        # If you remove the ignore type comment you will get the following error:
        #
        #   Argument 1 to "format_query" has incompatible type
        #   "ProcessableQuery[TSimpleDataSource]"; expected
        #   "Union[ProcessableQuery[SimpleDataSource], CompositeQuery[SimpleDataSource]]"
        #
        # This happens because self in this case is a generic type
        # CompositeQuery[TSimpleDataSource] while the function format_query takes a
        # SimpleDataSource (a concrete type). It is known by us (and mypy) that
        # TSimpleDataSource is bound to SimpleDataSource, which means that all types
        # parametrizing this class must be subtypes of SimpleDataSource, mypy is not smart
        # enough to know that though and so in order to have a generic repr function
        # I cast type check in this case.
        # Making TSimpleDataSource covariant would almost work except that covariant types
        # canot be used as parameters: https://github.com/python/mypy/issues/7049

        return "\n".join(format_query(cast(ProcessableQuery[SimpleDataSource], self)))
