from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional, Sequence

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query_dsl.accessors import get_time_range_expressions
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.storage import Storage, WritableTableStorage
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.query import Query
from snuba.query.conditions import ConditionFunctions, get_first_level_and_conditions
from snuba.query.data_source.join import JoinRelationship
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import AnyOptionalString
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or
from snuba.query.matchers import String as StringMatch
from snuba.query.processors import QueryProcessor
from snuba.query.validation import FunctionCallValidator


class Entity(ABC):
    """
    The Entity has access to multiple Storage objects, which represent the physical
    data model. Each one represents a table/view on the DB we can query.
    """

    def __init__(
        self,
        *,
        storages: Sequence[Storage],
        query_pipeline_builder: QueryPipelineBuilder[ClickhouseQueryPlan],
        abstract_column_set: ColumnSet,
        join_relationships: Mapping[str, JoinRelationship],
        writable_storage: WritableTableStorage,
        required_filter_columns: Optional[Sequence[str]],
        required_time_column: Optional[str],
    ) -> None:
        self.__storages = storages
        self.__query_pipeline_builder = query_pipeline_builder
        self.__writable_storage = writable_storage
        self.__data_model = abstract_column_set
        self.__join_relationships = join_relationships
        self._required_filter_columns = required_filter_columns
        self._required_time_column = required_time_column

    @abstractmethod
    def get_extensions(self) -> Mapping[str, QueryExtension]:
        """
        Returns the extensions for this entity.
        Every extension comes as an instance of QueryExtension.
        The schema tells Snuba how to parse the query.
        The processor actually does query processing for this extension.
        """
        # TODO: How does this work with JOINs?
        raise NotImplementedError("entity does not support queries")

    @abstractmethod
    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on the storage.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        return []

    def get_data_model(self) -> ColumnSet:
        """
        Now the data model is flat so this is just a simple ColumnSet object. We can expand this
        to also include relationships between entities.
        """
        return self.__data_model

    def get_join_relationship(self, relationship: str) -> Optional[JoinRelationship]:
        """
        Fetch the join relationship specified by the relationship string.
        """
        return self.__join_relationships.get(relationship)

    def get_all_join_relationships(self) -> Mapping[str, JoinRelationship]:
        """
        Returns all the join relationships
        """
        return self.__join_relationships

    def validate_required_conditions(
        self, query: Query, alias: Optional[str] = None
    ) -> bool:
        if not self._required_filter_columns and not self._required_time_column:
            return True

        condition = query.get_condition_from_ast()
        top_level = get_first_level_and_conditions(condition) if condition else []
        if not top_level:
            return False

        alias_match = AnyOptionalString() if alias is None else StringMatch(alias)

        def build_match(
            col: str, ops: Sequence[str], param_type: Any
        ) -> Or[Expression]:
            # The IN condition has to be checked separately since each parameter
            # has to be checked individually.
            column_match = ColumnMatch(alias_match, StringMatch(col))
            return Or(
                [
                    FunctionCallMatch(
                        Or([StringMatch(op) for op in ops]),
                        (column_match, LiteralMatch(AnyMatch(param_type))),
                    ),
                    FunctionCallMatch(
                        StringMatch(ConditionFunctions.IN),
                        (
                            column_match,
                            FunctionCallMatch(
                                Or([StringMatch("array"), StringMatch("tuple")]),
                                all_parameters=LiteralMatch(AnyMatch(param_type)),
                            ),
                        ),
                    ),
                ]
            )

        if self._required_filter_columns:
            for col in self._required_filter_columns:
                match = build_match(col, [ConditionFunctions.EQ], int)
                found = any(match.match(cond) for cond in top_level)
                if not found:
                    return False

        if self._required_time_column:
            match = build_match(
                self._required_time_column, [ConditionFunctions.EQ], datetime,
            )
            found = any(match.match(cond) for cond in top_level)
            if found:
                return True

            lower, upper = get_time_range_expressions(
                top_level, self._required_time_column, alias
            )
            if not lower or not upper:
                return False

            # At this point we have valid conditions. However we need to align them and
            # make sure they don't exceed the max_days. Replace the conditions.
            self._replace_time_condition(query, *lower, *upper)

        return True

    def _replace_time_condition(
        self,
        query: Query,
        from_date: datetime,
        from_exp: FunctionCall,
        to_date: datetime,
        to_exp: FunctionCall,
    ) -> None:
        max_days, date_align = state.get_configs(
            [("max_days", None), ("date_align_seconds", 1)]
        )

        def align_fn(dt: datetime) -> datetime:
            assert isinstance(date_align, int)
            return dt - timedelta(seconds=(dt - dt.min).seconds % date_align)

        from_date, to_date = align_fn(from_date), align_fn(to_date)
        assert from_date <= to_date

        if max_days is not None and (to_date - from_date).days > max_days:
            from_date = to_date - timedelta(days=max_days)

        def replace_cond(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp
            elif exp == from_exp:
                return replace(
                    exp, parameters=(from_exp.parameters[0], Literal(None, from_date)),
                )
            elif exp == to_exp:
                return replace(
                    exp, parameters=(to_exp.parameters[0], Literal(None, to_date))
                )

            return exp

        condition = query.get_condition_from_ast()
        if condition:
            query.set_ast_condition(condition.transform(replace_cond))

    def get_query_pipeline_builder(self) -> QueryPipelineBuilder[ClickhouseQueryPlan]:
        """
        Returns the component that orchestrates building and running query plans.
        """
        return self.__query_pipeline_builder

    def get_all_storages(self) -> Sequence[Storage]:
        """
        Returns all storages for this entity.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__storages

    def get_function_call_validators(self) -> Mapping[str, FunctionCallValidator]:
        """
        Provides a sequence of function expression validators for
        this entity. The typical use case is the validation that
        calls to entity specific functions are well formed.
        """
        return {}

    def get_writable_storage(self) -> WritableTableStorage:
        """
        Temporarily support getting the writable storage from an entity.
        Once consumers/replacers no longer reference entity, this can be removed
        and entity can have more than one writable storage.
        """
        return self.__writable_storage
