import random
from dataclasses import replace
from typing import cast

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.request.request_settings import RequestSettings
from snuba.state import get_config


def _validate_tupleElement_and_get_index(exp: FunctionCall):
    element_index_exp = exp.parameters[1]
    assert isinstance(element_index_exp, Literal)
    assert isinstance(element_index_exp.value, int)
    # clickhouse tuples are 1 -indexed, python tuples are not
    element_index = element_index_exp.value - 1
    assert (
        isinstance(exp.parameters[0], FunctionCall)
        and exp.parameters[0].function_name == "tuple"
    )
    return element_index


class _TupleElementerVisitor(ExpressionVisitor[Expression]):
    def visit_literal(self, exp: Literal) -> Expression:
        return exp

    def visit_column(self, exp: Column) -> Expression:
        return exp

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> Expression:
        return exp

    def visit_function_call(self, exp: FunctionCall) -> Expression:
        if exp.function_name == "tupleElement":
            # the tupleElement function's last parameter must be a constant:
            # https://clickhouse.com/docs/en/sql-reference/functions/tuple-functions#tupleelement

            element_index_exp = exp.parameters[1]
            if not isinstance(element_index_exp, Literal) or not isinstance(
                element_index_exp.value, int
            ):
                raise InvalidQueryException(
                    f"tupleElement must be indexed with an int literal, calculated values not supported, received {element_index_exp}"
                )
            # clickhouse tuples are 1 -indexed, python tuples are not
            element_index = element_index_exp.value - 1
            if (
                isinstance(exp.parameters[0], FunctionCall)
                and exp.parameters[0].function_name == "tuple"
                and isinstance(exp.parameters[0].parameters[element_index], Literal)
            ):
                to_return = exp.parameters[0].parameters[element_index]
                to_return.accept(self)
                return to_return
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        return replace(exp, parameters=transfomed_params)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> Expression:
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        res = replace(
            exp,
            internal_function=exp.internal_function.accept(self),
            parameters=transfomed_params,
        )
        return res

    def visit_lambda(self, exp: Lambda) -> Expression:
        res = replace(exp, transformation=exp.transformation.accept(self))
        return res

    def visit_argument(self, exp: Argument) -> Expression:
        return exp


class TupleElementer(QueryProcessor):
    """Resolves the `tupleElement function in a query

    e.g.: tupleElement(tuple("a", "b"), 1) -> "a"

    Thie is a zero cost function in clickhouse that is resolved at AST
    parse time. We do it on our end to avoid backwards incomaptibility issues.

    Only works with tuple Literals. WILL NOT WORK FOR CALCULATED TUPLES e.g:

    tupleElement(
        arrayJoin(
          arrayMap(
            (x,y ->
              tuple(
                x,
                y
              )
            ),
            tags.key,
            tags.value
          )
        ) AS `snuba_all_tags`,
        2
      ) AS `_snuba_tags_value` |> tags_value

    Due to a backwards incomaptibility issue in clickhouse from
    20.8 -> 21.8, tupleElement calls cause parsing errors
    parsing errors in certain conditions.
    """

    def should_run(self) -> bool:
        try:
            unaliaser_config_percentage = float(
                cast(float, get_config("tuple_elementer_rollout", 0))
            )
            return random.random() < unaliaser_config_percentage
        except ValueError:
            return False

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        if not self.should_run():
            return None
        visitor = _TupleElementerVisitor()
        query.transform(visitor)
