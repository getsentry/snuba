from dataclasses import replace

from snuba.clickhouse.query import Query
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    DangerousRawSQL,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    JsonPath,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class _TupleUnaliasVisitor(ExpressionVisitor[Expression]):
    def __init__(self) -> None:
        # TODO: make level handling a context manager maybe
        self.__level = 0

    def visit_literal(self, exp: Literal) -> Expression:
        return exp

    def visit_column(self, exp: Column) -> Expression:
        return exp

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> Expression:
        return exp

    def visit_function_call(self, exp: FunctionCall) -> Expression:
        self.__level += 1
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        self.__level -= 1
        if self.__level != 0 and exp.function_name == "tuple" and exp.alias is not None:
            return replace(exp, alias=None, parameters=transfomed_params)
        return replace(exp, parameters=transfomed_params)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> Expression:
        self.__level += 1
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        res = replace(
            exp,
            internal_function=exp.internal_function.accept(self),
            parameters=transfomed_params,
        )
        self.__level -= 1
        return res

    def visit_lambda(self, exp: Lambda) -> Expression:
        self.__level += 1
        res = replace(exp, transformation=exp.transformation.accept(self))
        self.__level -= 1
        return res

    def visit_argument(self, exp: Argument) -> Expression:
        return exp

    def visit_dangerous_raw_sql(self, exp: DangerousRawSQL) -> Expression:
        return exp

    def visit_json_path(self, exp: JsonPath) -> Expression:
        return replace(exp, base=exp.base.accept(self))


class TupleUnaliaser(ClickhouseQueryProcessor):
    """removes aliases of tuples nested inside other functions

        Due to a backwards incomaptibility issue in clickhouse from
        20.8 -> 21.8, aliased tuples within functions sometimes cause
        parsing errors in certain conditions.

        Issue: https://github.com/ClickHouse/ClickHouse/issues/35625

        This query processor removes the aliases of
        `tuple` function calls whenever they are nested within other expressions

        !!!WARNING!!!!
        This approach makes the assumption that aliases are expanded by the time they reach the query
        processor. At time of writing (2022-04-20 (blaze it)), this is done right after parsing in the
        pipeline. If aliases were no longer expanded,
        this processor would create invalid queries. For example:

        greater(
            multiIf(
              equals(
                tupleElement(
    alias declared ------> (tuple('$S', -1337) AS project_threshold_config),
                  -1337
                ),
                '$S'
              ),
              ` measurements [ lcp ] `,
              duration
            ),
            multiply(
    alias used --------> tupleElement(project_threshold_config, -1337),
              -1337
            )
          )

        if the project_threshold_config were not expanded earlier, the alias would not be found when
        executing the query
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        visitor = _TupleUnaliasVisitor()
        query.transform(visitor)
