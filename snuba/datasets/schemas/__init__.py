from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Mapping, Sequence

from snuba.clickhouse.columns import ColumnSet, ColumnType, Nullable, TModifiers
from snuba.query.expressions import FunctionCall


# TODO: Remove this abstraction entirely. It is not providing anything
# anymore since we are not making the query class depend on this and
# we are not using it anymore to create tables.
class RelationalSource(ABC):
    """
    Abstract representation of the datamodel in the schema.

    This class and its subclasses are the go-to place to inspect the structure
    of the datamodel, either during query or when writing.
    """

    @abstractmethod
    def get_columns(self) -> ColumnSet:
        raise NotImplementedError

    @abstractmethod
    def get_mandatory_conditions(self) -> Sequence[FunctionCall]:
        """
        Returns the mandatory conditions to apply on Clickhouse when
        querying this RelationalSource, if any.
        These conditions are supposed to be only meant to keep the data
        model consistent (like excluding rows that were tombstoned).
        """
        raise NotImplementedError


class Schema(ABC):
    """
    A schema is an abstraction over a data model we can query.
    It provides a set of columns and a where clause to build the query.
    Concretely this can represent a table, a view or a group of
    joined tables.
    This level of abstraction only provides read primitives. Subclasses provide
    a way to write and the DDL to build the datamodel on Clickhouse

    As of now we do not have a strict separation between a Snuba abstract
    schema and a Clickhouse concrete schema. When this will exist, this
    class will break up into snuba schema and clickhouse schema.
    """

    @abstractmethod
    def get_data_source(self) -> RelationalSource:
        """
        Returns an object that represent the tables and the relationship between
        tables in this Schema.
        """
        raise NotImplementedError

    def get_columns(self) -> ColumnSet:
        return self.get_data_source().get_columns()

    def get_column_differences(
        self, expected_columns: Mapping[str, ColumnType[TModifiers]]
    ) -> List[str]:
        """
        Returns a list of differences between the expected_columns and the columns described in the schema.
        """
        errors: List[str] = []

        for column in self.get_columns():
            if column.flattened not in expected_columns:
                errors.append(
                    "Column '%s' exists in schema but not local ClickHouse!"
                    % column.name
                )
                continue

            expected_type = expected_columns[column.flattened]

            if column.type.get_raw() != expected_type.get_raw() or column.type.has_modifier(
                Nullable
            ) != expected_type.has_modifier(
                Nullable
            ):
                errors.append(
                    "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)"
                    % (column.name, expected_type, column)
                )

        return errors
