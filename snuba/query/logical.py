from __future__ import annotations

from abc import ABCMeta
from typing import Any, Callable, Iterable, Optional, Sequence, Type, Union, cast

from snuba.query import LimitBy, OrderBy, ProcessableQuery, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause
from snuba.query.data_source.simple import Entity, LogicalDataSource, Storage
from snuba.query.expressions import Expression, ExpressionVisitor


class Query(ProcessableQuery[LogicalDataSource]):
    """
    Represents the logical query during query processing.
    This means the query class used between parsing and query translation.

    A logical query can have either an Entity or a Storage as it's from_clause.

    If a function needs to operate on entities or storages specifically,
    use the StorageQuery, EntityQuery classes specified below
    """

    def __init__(
        self,
        from_clause: Optional[LogicalDataSource],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        sample: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
    ):
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__final = False
        self.__sample = sample

        super().__init__(
            from_clause=from_clause,
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

    def get_final(self) -> bool:
        return self.__final

    def set_final(self, final: bool) -> None:
        self.__final = final

    def get_sample(self) -> Optional[float]:
        return self.__sample

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_final", "get_sample")

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass


class _FlexibleQueryType(ABCMeta):
    def __call__(cls, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            f"{cls.__name__} class cannot be instantiated directly, use snuba.query.logical.Query"
        )

    def __instancecheck__(self, instance: Any) -> bool:
        if isinstance(instance, ProcessableQuery):
            data_source_type = cast(type, getattr(self, "data_source", object)())
            instance_data_source = instance.get_from_clause()
            return isinstance(instance_data_source, data_source_type)
        else:
            return False


"""
Below are two query classes which can be used as hints to the type checker but not instantiated directly

They exist for the following reason:

There are parts of the codebase which can ONLY operate on entity queries or storage queries, but MOST
operations that operate on queries do not care about the data source (e.g. they are transforming some expression)

For those parts of the codebase that need that specificity, functions can be typed like this:

>>> def my_entity_transformation(query: EntityQuery):
>>>    pass

similar results can be achieved by writing:

>>> def my_entity_transformation(query: ProcessableQuery[Entity]):
>>>    pass

However it cannot be asserted at runtime that

>>> isinstance(query, ProcessableQuery[Entity])

mypy generics are not present at runtime and there are lot of checks in this codebase that look like:


>>> isinstance(query, EntityQuery):
>>>     do_something()

These classes give us the best of both worlds by allowing the strict typing and the runtime checking which makes sure
that when someone is passing in an EntityQuery its datasource is actually an Entity

"""


class EntityQuery(Query, metaclass=_FlexibleQueryType):
    @classmethod
    def data_source(cls) -> Type[Entity]:
        return Entity

    def get_from_clause(self) -> Entity:
        return cast(Entity, super().get_from_clause())

    @classmethod
    def check_data_source(
        cls,
        data_source: Union[
            Query,
            ProcessableQuery[Entity],
            CompositeQuery[Entity],
            JoinClause[Entity],
            IndividualNode[Entity],
        ],
    ) -> None:
        if isinstance(data_source, JoinClause):
            if isinstance(data_source.left_node, IndividualNode):
                cls.check_data_source(data_source.left_node)
            if isinstance(data_source.right_node, IndividualNode):
                cls.check_data_source(data_source.right_node)
        elif isinstance(data_source, IndividualNode):
            assert isinstance(data_source.data_source, cls.data_source())
        elif isinstance(data_source, CompositeQuery):
            cls.check_data_source(data_source.get_from_clause())
        else:
            assert isinstance(data_source.get_from_clause(), cls.data_source())

    @classmethod
    def from_query(cls, query: Union[Query, CompositeQuery[Entity]]) -> "EntityQuery":
        cls.check_data_source(query)
        return cast("EntityQuery", query)


class StorageQuery(Query, metaclass=_FlexibleQueryType):
    @classmethod
    def data_source(cls) -> Type[Storage]:
        return Storage

    def get_from_clause(self) -> Storage:
        return cast(Storage, super().get_from_clause())

    @classmethod
    def from_query(cls, query: Query) -> "StorageQuery":
        assert isinstance(query.get_from_clause(), cls.data_source())
        return cast("StorageQuery", query)
