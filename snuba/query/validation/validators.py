from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional, Sequence, Type, cast

from snuba.clickhouse.translators.snuba.mappers import ColumnToExpression
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.environment import metrics as environment_metrics
from snuba.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    build_match,
    get_first_level_and_conditions,
)
from snuba.query.exceptions import InvalidExpressionException, InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.expressions import SubscriptableReference as SubscriptableReferenceExpr
from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import Or
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import RegisteredClass
from snuba.utils.schemas import ColumnSet, Date, DateTime

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment_metrics, "query.validators")


class ColumnValidationMode(Enum):
    DO_NOTHING = 0
    WARN = 1
    ERROR = 2


class QueryValidator(ABC, metaclass=RegisteredClass):
    """
    Contains validation logic that requires the entire query. An entity has one or more
    of these validators that it adds contextual information too.

    WARNING!!!

    This class assumes that all of its subclasses are in this same file in order for the
    RegisteredClass functionality to work. If validators are defined in other files and
    not imported, they would not be picked up and the `get_from_name` function would not work
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["QueryValidator"]:
        return cast(Type["QueryValidator"], cls.class_from_name(name))

    @abstractmethod
    def validate(
        self,
        query: Query,
        alias: Optional[str] = None,
    ) -> None:
        """
        Validate that the query is correct. If the query is not valid, raise an
        Exception, otherwise return None. If the entity that calls this is part
        of a join query, the alias will be populated with the entity's alias.

        :param query: The query to validate.
        :type query: Query
        :param alias: The alias of the entity in a JOIN query.
        :type alias: Optional[str]
        :raises InvalidQueryException: [description]
        """
        raise NotImplementedError


class EntityRequiredColumnValidator(QueryValidator):
    """
    Certain entities require the Query to filter by certain required columns.
    This validator checks if the Query contains filters by all of the required columns.
    """

    def __init__(self, required_filter_columns: Sequence[str]) -> None:
        self.required_columns = set(required_filter_columns)

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []

        missing = set()
        if self.required_columns:
            for col in self.required_columns:
                match = build_match(
                    col=col, ops=[ConditionFunctions.EQ], param_type=int, alias=alias
                )
                found = any(match.match(cond) for cond in top_level)
                if not found:
                    missing.add(col)

        if missing:
            raise InvalidQueryException(
                f"missing required conditions for {', '.join(missing)}"
            )


class EntityContainsColumnsValidator(QueryValidator):
    """
    Ensures that all columns in the query actually exist in the entity.
    """

    def __init__(
        self,
        entity_data_model: EntityColumnSet,
        mappers: list[TranslationMappers],
        validation_mode: ColumnValidationMode,
    ) -> None:
        self.validation_mode = validation_mode
        self.entity_data_model = entity_data_model
        # The entity can accept some column names that get mapped to other expressions
        # Parse and store those mappings as well
        self.mapped_columns = set()
        for mapper in mappers:
            for colmapping in mapper.columns:
                if isinstance(colmapping, ColumnToExpression):
                    self.mapped_columns.add(colmapping.from_col_name)

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        if self.validation_mode == ColumnValidationMode.DO_NOTHING:
            return

        query_columns = query.get_all_ast_referenced_columns()

        missing = set()
        for column in query_columns:
            if (
                column.table_name == alias
                and column.column_name not in self.entity_data_model
                and column.column_name not in self.mapped_columns
            ):
                missing.add(column.column_name)

        if missing:
            error_message = f"query column(s) {', '.join(missing)} do not exist"
            if self.validation_mode == ColumnValidationMode.ERROR:
                raise InvalidQueryException(error_message)
            elif self.validation_mode == ColumnValidationMode.WARN:
                logger.warning(error_message, exc_info=True)


class NoTimeBasedConditionValidator(QueryValidator):
    """
    For some logic (e.g. subscriptions) we want to make sure that there are no time conditions
    on the query, so we can add conditions and ensure a certain time range is being queried.
    This validator will scan the query for any top level conditions on the specified time
    column and ensure there are no conditions.
    """

    def __init__(self, required_time_column: str) -> None:
        self.required_time_column = required_time_column
        self.match = build_match(
            col=required_time_column,
            ops=[
                ConditionFunctions.EQ,
                ConditionFunctions.LT,
                ConditionFunctions.LTE,
                ConditionFunctions.GT,
                ConditionFunctions.GTE,
            ],
            param_type=datetime,
        )

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []
        for cond in top_level:
            if self.match.match(cond):
                raise InvalidExpressionException.from_args(
                    cond,
                    f"Cannot have existing conditions on time field {self.required_time_column}",
                    should_report=False,
                )


class SubscriptionAllowedClausesValidator(QueryValidator):
    """
    Subscriptions expect a very specific query structure. This will ensure that only the allowed
    clauses are being used in the query, and that those clauses are in the correct structure.
    """

    def __init__(
        self, max_allowed_aggregations: int, disallowed_aggregations: Sequence[str]
    ) -> None:
        self.max_allowed_aggregations = max_allowed_aggregations
        self.disallowed_aggregations = disallowed_aggregations

    @staticmethod
    def _validate_groupby_fields_have_matching_conditions(
        query: Query, alias: Optional[str] = None
    ) -> None:
        """
        Method that insures that for every field in the group by clause, there should be a
        matching a condition. For example, if we had in our groupby clause [project_id, tags[3]],
        we should have the following conditions in the where clause `project_id = 3 AND tags[3]
        IN array(1,2,3)`. This is necessary because we want to avoid the case where an
        unspecified number of buckets is returned.
        """
        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []
        for exp in query.get_groupby():
            if isinstance(exp, SubscriptableReferenceExpr):
                match = build_match(
                    subscriptable=str(exp.column.column_name),
                    ops=[ConditionFunctions.EQ],
                    param_type=int,
                    alias=alias,
                    key=str(exp.key.value),
                )
            elif isinstance(exp, Column):
                match = build_match(
                    col=exp.column_name,
                    ops=[ConditionFunctions.EQ],
                    param_type=int,
                    alias=alias,
                    key=None,
                )
            else:
                raise InvalidQueryException(
                    "Unhandled column type in group by validation"
                )

            found = any(match.match(cond) for cond in top_level)

            if not found:
                raise InvalidQueryException(
                    f"Every field in groupby must have a corresponding condition in "
                    f"where clause. missing condition for field {exp}"
                )

    def validate(
        self,
        query: Query,
        alias: Optional[str] = None,
    ) -> None:
        selected = query.get_selected_columns()
        if len(selected) > self.max_allowed_aggregations:
            aggregation_error_text = (
                "1 aggregation is"
                if self.max_allowed_aggregations == 1
                else f"{self.max_allowed_aggregations} aggregations are"
            )
            raise InvalidQueryException(
                f"A maximum of {aggregation_error_text} allowed in the select"
            )

        for field in self.disallowed_aggregations:
            if getattr(query, f"get_{field}")():
                raise InvalidQueryException(
                    f"invalid clause {field} in subscription query"
                )

        if "groupby" not in self.disallowed_aggregations:
            self._validate_groupby_fields_have_matching_conditions(query, alias)


class GranularityValidator(QueryValidator):
    """Verify that the given granularity is a multiple of the configured value"""

    def __init__(self, minimum: int, required: bool = False):
        self.minimum = minimum
        self.required = required

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        granularity = query.get_granularity()
        if granularity is None:
            if self.required:
                raise InvalidQueryException("Granularity is missing")
        elif granularity < self.minimum or (granularity % self.minimum) != 0:
            raise InvalidQueryException(
                f"granularity must be multiple of {self.minimum}"
            )


class TagConditionValidator(QueryValidator):
    """
    Sometimes a query will have a condition that compares a tag value to a
    non-string literal. This will always fail (tags are always strings) so
    catch this specific case and reject it.
    """

    def __init__(self) -> None:
        self.condition_matcher = build_match(
            subscriptable="tags",
            ops=[
                ConditionFunctions.EQ,
                ConditionFunctions.NEQ,
                ConditionFunctions.LTE,
                ConditionFunctions.GTE,
                ConditionFunctions.LT,
                ConditionFunctions.GT,
                ConditionFunctions.LIKE,
                ConditionFunctions.NOT_LIKE,
            ],
            array_ops=[ConditionFunctions.IN, ConditionFunctions.NOT_IN],
        )

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        if not condition:
            return
        for cond in condition:
            match = self.condition_matcher.match(cond)
            if match:
                column = match.expression("column")
                col_str: str
                if not isinstance(column, SubscriptableReferenceExpr):
                    return  # only fail things on the tags[] column

                col_str = f"{column.column.column_name}[{column.key.value}]"
                error_prefix = f"invalid tag condition on '{col_str}':"

                rhs = match.expression("rhs")
                if isinstance(rhs, Literal):
                    if not isinstance(rhs.value, str):
                        raise InvalidQueryException(
                            f"{error_prefix} {rhs.value} must be a string"
                        )
                elif isinstance(rhs, FunctionCall):
                    # The rhs is guaranteed to be an array function because of the match
                    for param in rhs.parameters:
                        if isinstance(param, Literal) and not isinstance(
                            param.value, str
                        ):
                            raise InvalidQueryException(
                                f"{error_prefix} array literal {param.value} must be a string"
                            )


class DatetimeConditionValidator(QueryValidator):
    def __init__(self) -> None:
        self.matchers: list[Or[Expression]] = []

    def initialize(self, schema: ColumnSet) -> None:
        for column in schema.columns:
            if isinstance(column.type, (DateTime, Date)):
                matcher = build_match(
                    col=column.name,
                    ops=[
                        ConditionFunctions.EQ,
                        ConditionFunctions.LT,
                        ConditionFunctions.LTE,
                        ConditionFunctions.GT,
                        ConditionFunctions.GTE,
                    ],
                )
                self.matchers.append(matcher)

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        if not isinstance(query, LogicalQuery):
            return  # TODO: This doesn't work for queries with multiple entities.

        # We can safely initialize this once and store the value, since these validators are
        # created for each entity that uses them and are always used by the same entity.
        # Also since this doesn't run on composite queries, the alias will always be None
        # and doesn't affect the underlying matcher.
        if not self.matchers:
            self.initialize(query.get_from_clause().get_columns())

        condition = query.get_condition()
        if condition:
            for cond in condition:
                for matcher in self.matchers:
                    match = matcher.match(cond)
                    if match:
                        rhs = match.expression("rhs")
                        if isinstance(rhs, Literal):
                            if not isinstance(rhs.value, datetime):
                                lhs = match.expression("column")
                                # TODO: change this to a proper exception after ensuring the product isn't
                                # passing bad queries
                                metrics.increment(
                                    "datetime_condition_error",
                                    tags={
                                        "column": str(lhs),
                                        "entity": query.get_from_clause().key.value,
                                    },
                                )
                                logger.warning(
                                    f"{lhs} requires datetime conditions: '{rhs.value}' is not a valid datetime"
                                )
                        elif isinstance(rhs, FunctionCall):
                            # The matcher only matches array/tuples of literals
                            for param in rhs.parameters:
                                if isinstance(param, Literal) and not isinstance(
                                    param.value, datetime
                                ):
                                    lhs = match.expression("column")
                                    # TODO: change this to a proper exception after ensuring the product isn't
                                    # passing bad queries
                                    metrics.increment(
                                        "datetime_condition_error",
                                        tags={
                                            "column": str(lhs),
                                            "entity": query.get_from_clause().key.value,
                                        },
                                    )
                                    logger.warning(
                                        f"{lhs} requires datetime conditions: '{param.value}' is not a valid datetime"
                                    )
