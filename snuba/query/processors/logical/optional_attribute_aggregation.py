from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class OptionalAttributeAggregationTransformer(LogicalQueryProcessor):
    """When aggregating dynamic fields, we have to take into account that not every row will have said field.
    This processor adjusts the query to account for the dynamic value not being present

    avg(attr_num["num_clicks"]) -- becomes --> avgIf(attr_num["num_clicks"], mapContains(attr_num, num_clicks))


    Example:
        A customer attaches a `num_clicks` attribute to spans during the regular flow.
        It is stored in the attr_num field on the entity such that
        SELECT attr_num['num_clicks'] yields a value !!BUT!! that value may not be present in all spans

        Now if we did an aggregation like `SELECT avg(attr_num['num_clicks']) FROM spans`, our data would be incorrect
        becase all the spans which did not have the `num_clicks` value in its `attr_num` map, would be defaulted to 0, thus
        skewing the average
    """

    def __init__(
        self,
        attribute_column_names: list[str],
        aggregation_names: list[str],
        curried_aggregation_names: list[str],
    ):
        self._attribute_column_names = attribute_column_names
        self._aggregation_names = aggregation_names
        self._curried_aggregation_names = curried_aggregation_names

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def is_attribute_expression(exp_parameters: tuple[Expression, ...]) -> bool:
            if len(exp_parameters) != 1:
                return False
            func_param = exp_parameters[0]
            if not isinstance(func_param, SubscriptableReference):
                return False
            if func_param.column.column_name in self._attribute_column_names:
                return True
            return False

        def transform_aggregates_to_conditionals(exp: Expression) -> Expression:
            if (
                isinstance(exp, FunctionCall)
                and exp.function_name in self._aggregation_names
                and len(exp.parameters) == 1
                and is_attribute_expression(exp.parameters)
            ):
                assert isinstance(exp.parameters[0], SubscriptableReference)
                return FunctionCall(
                    alias=exp.alias,
                    function_name=f"{exp.function_name}If",
                    parameters=(
                        exp.parameters[0],
                        FunctionCall(
                            alias=None,
                            function_name="mapContains",
                            parameters=(
                                exp.parameters[0].column,
                                exp.parameters[0].key,
                            ),
                        ),
                    ),
                )

            elif isinstance(exp, CurriedFunctionCall):
                if (
                    exp.internal_function.function_name
                    in self._curried_aggregation_names
                    and is_attribute_expression(exp.parameters)
                ):
                    assert isinstance(exp.parameters[0], SubscriptableReference)
                    return CurriedFunctionCall(
                        alias=exp.alias,
                        internal_function=FunctionCall(
                            alias=None,
                            function_name=f"{exp.internal_function.function_name}If",
                            parameters=exp.internal_function.parameters,
                        ),
                        parameters=(
                            exp.parameters[0],
                            FunctionCall(
                                alias=None,
                                function_name="mapContains",
                                parameters=(
                                    exp.parameters[0].column,
                                    exp.parameters[0].key,
                                ),
                            ),
                        ),
                    )

            return exp

        query.transform_expressions(transform_aggregates_to_conditionals)
