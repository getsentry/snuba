from dataclasses import dataclass
from typing import Optional, Tuple, Union

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    CurriedFunctionCallMapper,
    FunctionCallMapper,
)
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import CurriedFunctionCall, Expression, FunctionCall


def _build_parameters(
    expression: Union[FunctionCall, CurriedFunctionCall],
    children_translator: SnubaClickhouseStrictTranslator,
    aggregated_col_name: str,
) -> Tuple[Expression, ...]:
    assert isinstance(expression.parameters[0], ColumnExpr)
    return (
        ColumnExpr(None, expression.parameters[0].table_name, aggregated_col_name),
        *[p.accept(children_translator) for p in expression.parameters[1:]],
    )


def _should_transform_aggregation(
    function_name: str,
    expected_function_name: str,
    column_to_map: str,
    function_call: Union[FunctionCall, CurriedFunctionCall],
) -> bool:
    return (
        function_name == expected_function_name
        and len(function_call.parameters) > 0
        and isinstance(function_call.parameters[0], ColumnExpr)
        and function_call.parameters[0].column_name == column_to_map
    )


@dataclass(frozen=True)
class AggregateFunctionMapper(FunctionCallMapper):
    """
    Turns expressions like max(value) into maxMerge(max)
    or maxIf(value, condition) into maxMergeIf(max, condition)
    """

    column_to_map: str
    from_name: str
    to_name: str
    aggr_col_name: str

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if not _should_transform_aggregation(
            expression.function_name, self.from_name, self.column_to_map, expression
        ):
            return None

        return FunctionCall(
            expression.alias,
            self.to_name,
            _build_parameters(expression, children_translator, self.aggr_col_name),
        )


@dataclass(frozen=True)
class AggregateCurriedFunctionMapper(CurriedFunctionCallMapper):
    """
    Turns expressions like quantiles(0.9)(value) into quantilesMerge(0.9)(percentiles)
    or quantilesIf(0.9)(value, condition) into quantilesMergeIf(0.9)(percentiles, condition)
    """

    column_to_map: str
    from_name: str
    to_name: str
    aggr_col_name: str

    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[CurriedFunctionCall]:
        if not _should_transform_aggregation(
            expression.internal_function.function_name,
            self.from_name,
            self.column_to_map,
            expression,
        ):
            return None

        return CurriedFunctionCall(
            expression.alias,
            FunctionCall(
                None,
                self.to_name,
                tuple(
                    p.accept(children_translator) for p in expression.internal_function.parameters
                ),
            ),
            _build_parameters(expression, children_translator, self.aggr_col_name),
        )
