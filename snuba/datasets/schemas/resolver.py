from abc import ABC, abstractmethod
from typing import Mapping, NamedTuple, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.join import JoinNode


class ResolvedCol(NamedTuple):
    table_name: Optional[str]
    column_name: str
    path: Sequence[str]


class ColumnResolver(ABC):
    """
    A ColumnResolver is a component that knows the logical schema of a data set and is capable
    of resolving tables and nested columns from the representation we receive in the query.

    This assumes the parser does not have enough information from the query language alone to
    decompose a column expression found in a query into entity (table), base column name and
    path for nested columns. Should we change the query language in such a way that a reference
    to a column had non ambiguous information about entity name and nested columns this will
    become a simple validator.

    TODO: Revisit this interface when we introduce entities. We may have to increase its
    responsibilities depending on how we decide to infer, given a column, which entity defines it.
    If we require the query to fully qualify all columns (errors.message for example), this can
    stay as it is. If instead we will infer the requested entity from the context of the query
    like we do today this will need to know a lot more about the query, be coupled to that
    structure and being potentially able to make wider changes to the query itself.
    Moving such responsibility here would be easy enough that we can keep this simpler till
    entities are introduced.
    """

    @abstractmethod
    def resolve_column(self, query_column: str) -> Optional[ResolvedCol]:
        """
        Transforms the column string found in the query into a ResolvedCol if such expression is
        valid with respect to the logical schema of the dataset. If not it returns None.

        This does not return a Column object to avoid having to deal with the alias, that this
        class should not have the right of changing.
        """
        raise NotImplementedError


def _resolve_column_in_set(
    table_name: Optional[str],
    column_set: ColumnSet,
    virtual_column_names: Sequence[str],
    column_name: str,
) -> Optional[ResolvedCol]:
    """
    Resolves a column expressed as `<base>.<path>` which is what our schema, the ColumnSet
    class and Clickhouse itself currently support. The query language is fairly agnostic
    to this limitation.
    ColumnSet would not support multi-level nesting (col.nested.more_nested), nor flat column
    names with `.` inside (my.column.name). So if Clickhouse first and our schema abstraction
    started supporting more flexible nesting, this implementation would have to be expanded.
    """
    flattened_col = column_set.get(column_name)
    if flattened_col:
        return ResolvedCol(
            table_name=table_name,
            # FlattenedColumn.name is the name of the column for non nested columns while
            # it is the first and only level of nesting if the column is nested.
            column_name=flattened_col.base_name or flattened_col.name,
            path=[flattened_col.name] if flattened_col.base_name else [],
        )
    elif (
        # This `any` call is to identify nested column referenced with the base name without
        # a path (like `tags`). That is not present at all as a flattened column in
        # the ColumnSet object.
        any(c for c in column_set.columns if c.name == column_name)
        or column_name in virtual_column_names
    ):
        return ResolvedCol(table_name=table_name, column_name=column_name, path=[])
    else:
        return None


class SingleTableResolver(ColumnResolver):
    def __init__(
        self,
        columns: ColumnSet,
        virtual_column_names: Optional[Sequence[str]] = None,
        table_name: Optional[str] = None,
    ) -> None:
        self.__columns = columns
        # virtual_column_names includes those columns we accept in a query for a dataset that
        # are not part of the schema officially. Examples tags_key and tags_value and that's
        # it.
        # TODO: Consider moving the resolution of tags_key/tags_value to arrayJoin before
        # column resolution.
        self.__virtual_column_names = virtual_column_names or []
        self.__table_name = table_name

    def resolve_column(self, query_column: str) -> Optional[ResolvedCol]:
        return _resolve_column_in_set(
            self.__table_name, self.__columns, self.__virtual_column_names, query_column
        )


class JoinedTablesResolver(ColumnResolver):
    """
    Resolves columns in a join query where all columns are supposed to be fully qualified
    with the table alias prepended.
    """

    def __init__(
        self, join_root: JoinNode, virtual_column_names: Mapping[str, Sequence[str]],
    ) -> None:
        self.__join_root = join_root
        self.__virtual_column_names = virtual_column_names

    def resolve_column(self, query_column: str) -> Optional[ResolvedCol]:
        split_col = query_column.split(".")
        table = self.__join_root.get_tables().get(split_col[0])
        if table is None:
            return None
        return _resolve_column_in_set(
            split_col[0],
            table.get_columns(),
            self.__virtual_column_names.get(split_col[0], []),
            ".".join(split_col[1:]),
        )
