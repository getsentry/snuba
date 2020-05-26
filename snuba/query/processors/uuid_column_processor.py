from typing import Set
import uuid

from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.conditions import (
    binary_condition,
    is_condition,
    is_in_condition,
    is_not_in_condition,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class UUIDColumnProcessor(QueryProcessor):
    """
    If a condition is being performed on a column that stores UUIDs (as defined in the constructor)
    then change the condition to use a proper UUID instead of a string.
    """

    def __init__(self, uuid_columns: Set[str]) -> None:
        self.__uuid_columns = uuid_columns

    def is_uuid_column(self, exp: Expression) -> bool:
        return isinstance(exp, Column) and exp.column_name in self.__uuid_columns

    def is_formatted_uuid(self, exp: Expression) -> bool:
        return (
            isinstance(exp, FunctionCall)
            and exp.function_name == "replaceAll"
            and isinstance(exp.parameters[0], FunctionCall)
            and exp.parameters[0].function_name == "toString"
            and isinstance(exp.parameters[0].parameters[0], Column)
            and exp.parameters[0].parameters[0].column_name in self.__uuid_columns
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_condition(exp: Expression) -> Expression:
            if is_condition(exp):
                assert isinstance(exp, FunctionCall)
                if len(exp.parameters) > 1:
                    # This doesn't seem great but here it is: There is a separate processor that converts
                    # all cases of UUID columns to do a string comparison. We want to to undo that work
                    # for conditions only, so we need to handle the common case (no formatting) as well
                    # as the case that the other processor has formatted the column already.

                    # Check if we have a uuid column
                    if any(
                        self.is_uuid_column(param) or self.is_formatted_uuid(param)
                        for param in exp.parameters
                    ):
                        new_params = []
                        if is_in_condition(exp) or is_not_in_condition(exp):
                            param = exp.parameters[0]
                            if self.is_formatted_uuid(param):
                                column = param.parameters[0].parameters[0]
                                new_params.append(
                                    Column(
                                        param.alias,
                                        column.table_name,
                                        column.column_name,
                                    )
                                )
                            else:
                                new_params.append(param)

                            literal_fn = exp.parameters[1]
                            new_tuple = []
                            for lit in literal_fn.parameters:
                                try:
                                    parsed = uuid.UUID(lit.value)
                                    new_tuple.append(Literal(lit.alias, str(parsed)))
                                except Exception:
                                    new_tuple.append(lit)

                            new_params.append(
                                FunctionCall(
                                    literal_fn.alias,
                                    literal_fn.function_name,
                                    tuple(new_tuple),
                                )
                            )
                        else:
                            for param in exp.parameters:
                                if self.is_formatted_uuid(param):
                                    column = param.parameters[0].parameters[0]
                                    new_params.append(
                                        Column(
                                            param.alias,
                                            column.column_name,
                                            column.table_name,
                                        )
                                    )
                                elif isinstance(param, Literal):
                                    try:
                                        parsed = uuid.UUID(param.value)
                                        new_params.append(
                                            Literal(param.alias, str(parsed))
                                        )
                                    except Exception:
                                        new_params.append(param)
                                else:
                                    new_params.append(param)

                        return binary_condition(
                            exp.alias, exp.function_name, *new_params
                        )

            return exp

        query.transform_expressions(process_condition)
