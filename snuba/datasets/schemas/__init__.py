from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Mapping, List

from snuba.clickhouse.columns import ColumnSet


class RelationalSource(ABC):
    """
    Represent an abstract view over a relational datasource, which
    can represent a table or a join.
    """

    @abstractmethod
    def copy(self) -> RelationalSource:
        raise NotImplementedError

    @abstractmethod
    def format(self) -> str:
        # Not using the __str__ method because this is moving towards a more
        # abstract method that will receive a FormatStrategy (clickhouse specific)
        # that would do the actual formatting work
        raise NotImplementedError


class Schema(ABC):
    """
    A schema is an abstraction over a data model we can query.
    It provides a set of columns and a where clause to build the query.
    Concretely this can represent a table, a view or a group of
    joined tables.
    This level of abstraction only provides read primitives.

    As of now we do not have a strict separation between a Snuba abstract
    schema and a Clickhouse concrete schema. When this will exist, this
    class will break up into snuba schema and clickhouse schema.
    """

    def __init__(
        self,
        columns: ColumnSet,
    ) -> None:
        self.__columns = columns

    @abstractmethod
    def get_data_source(self) -> RelationalSource:
        """
        Builds and returns the content of the FROM clause Clickhouse
        needs in order to execute a query on this schema.
        This can be a simple table or a view for simple dataset
        or the join clause for joined datasets.
        """
        raise NotImplementedError

    def get_columns(self) -> ColumnSet:
        return self.__columns

    def get_column_differences(self, expected_columns: Mapping[str, str]) -> List[str]:
        """
        Returns a list of differences between the expected_columns and the columns described in the schema.
        """
        errors: List[str] = []

        for column_name, column_type in expected_columns.items():
            if column_name not in self.__columns:
                errors.append("Column '%s' exists in local ClickHouse but not in schema!" % column_name)
                continue

            expected_type = self.__columns[column_name].type.for_schema()
            if column_type != expected_type:
                errors.append(
                    "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)" % (
                        column_name,
                        expected_type,
                        column_type
                    )
                )

        return errors
