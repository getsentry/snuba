from snuba import state
from snuba.query.conditions import (
    combine_and_conditions,
    get_first_level_and_conditions,
    is_condition,
)
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class ConditionSimplifierProcessor(LogicalQueryProcessor):
    """
    22.8 has some problems when dealing with certain conditions on strings.
    Specifically, a condition with multiple values on a string column,
    e.g. release IN tuple('a', 'b', 'c')
    will cause an error.
    THis processor does two things: if the rhs is a single value, change the condition
    to be equals(). Otherwise, flip the sides using the `has` operator.
    So has(tuple('a', 'b', 'c'), release) instead of in(release, tuple('a', 'b', 'c'))
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_cond(exp: Expression) -> Expression:
            if not is_condition(exp):
                return exp

            assert isinstance(exp, FunctionCall)
            if not exp.function_name == "in" or len(exp.parameters) != 2:
                return exp

            rhs = exp.parameters[1]
            if not isinstance(rhs, FunctionCall) or rhs.function_name not in (
                "tuple",
                "array",
            ):
                return exp

            if len(rhs.parameters) == 1:
                return FunctionCall(
                    exp.alias, "equals", (exp.parameters[0], rhs.parameters[0])
                )

            return FunctionCall(exp.alias, "has", (rhs, exp.parameters[0]))

        if state.get_int_config("use.condition.simplifier.processor", 1) == 0:
            return

        condition = query.get_condition()
        if condition:
            conditions = get_first_level_and_conditions(condition)
            new_conditions = [transform_cond(c) for c in conditions]
            query.set_ast_condition(combine_and_conditions(new_conditions))
