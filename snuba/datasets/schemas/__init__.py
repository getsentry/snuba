from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Mapping, NamedTuple, Sequence

from snuba.clickhouse.columns import ColumnSet, ColumnType
from snuba.query.expressions import Expression
from snuba.query.types import Condition


class MandatoryCondition(NamedTuple):
    legacy: Condition
    ast: Expression


class RelationalSource(ABC):
    """
    Abstract representation of the datamodel in the schema. This includes the
    list of the tables that compose this datamodel with their columns as well as
    their relationships expressed as relational joins.

    This class and its subclasses are the go-to place to inspect the structure
    of the datamodel, either during query or when writing.

    This implies our data model is defined in a relational way. Should we move
    away from this assumption, this will change.
    """

    @abstractmethod
    def format_from(self) -> str:
        """
        Builds the SQL representation of the data source for the FROM clause
        when querying.
        """
        # Not using the __str__ method because this is moving towards a more
        # abstract method that will receive a FormatStrategy (clickhouse specific)
        # that would do the actual formatting work
        raise NotImplementedError

    @abstractmethod
    def get_columns(self) -> ColumnSet:
        raise NotImplementedError

    @abstractmethod
    def get_mandatory_conditions(self) -> Sequence[MandatoryCondition]:
        """
        Returns the mandatory conditions to apply on Clickhouse when
        querying this RelationalSource, if any.
        These conditions are supposed to be only meant to keep the data
        model consistent (like excluding rows that were tombstoned).
        """
        raise NotImplementedError

    @abstractmethod
    def get_prewhere_candidates(self) -> Sequence[str]:
        """
        Returns the list of keys that can be promoted to PREWHERE conditions
        if found in the conditions field of the query.
        pre where keys depend on the actual table used to run the query, so,
        since the query processors can change the datasource of the query, the
        list of candidates must be associated to the data source itself and not
        to the dataset.
        """
        raise NotImplementedError

    def supports_sample(self) -> bool:
        """
        TODO: This is a temporary method to prevent Clickhouse query to try to
        add the SAMPLE clause to a JOIN expression, where SAMPLE not only is formatted
        differently, but would have to be rethought since, in a join SAMPLE would
        have to be applied table by table.
        """
        return True


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
        self, expected_columns: Mapping[str, ColumnType]
    ) -> List[str]:
        """
        Returns a list of differences between the expected_columns and the columns described in the schema.
        """
        errors: List[str] = []

        for column_name, column in expected_columns.items():
            if column_name not in self.get_columns():
                errors.append(
                    "Column '%s' exists in local ClickHouse but not in schema!"
                    % column_name
                )
                continue

            expected_type = self.get_columns()[column_name].type

            if column != expected_type:
                errors.append(
                    "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)"
                    % (column_name, expected_type, column)
                )

        return errors
