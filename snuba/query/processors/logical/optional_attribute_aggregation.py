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
        print("huh_OptionalAttributeAggregationTransformer")
        self._attribute_column_names = attribute_column_names
        self._aggregation_names = aggregation_names
        self._curried_aggregation_names = curried_aggregation_names

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def find_subscriptable_reference(
            exp: Expression,
        ) -> SubscriptableReference | None:
            # Recursively find the SubscriptableReference in nested expressions
            if (
                isinstance(exp, SubscriptableReference)
                and exp.column.column_name in self._attribute_column_names
            ):
                return exp
            elif isinstance(exp, FunctionCall) and exp.parameters:
                for param in exp.parameters:
                    result = find_subscriptable_reference(param)
                    if result:
                        return result
            elif isinstance(exp, CurriedFunctionCall):
                for param in exp.parameters:
                    result = find_subscriptable_reference(param)
                    if result:
                        return result
            return None

        def transform_aggregates_to_conditionals(exp: Expression) -> Expression:
            if (
                isinstance(exp, FunctionCall)
                and exp.function_name in self._aggregation_names
            ):
                subscriptable_ref = find_subscriptable_reference(exp)
                if subscriptable_ref:
                    return FunctionCall(
                        alias=exp.alias,
                        function_name=f"{exp.function_name}If",
                        parameters=(
                            *exp.parameters,
                            FunctionCall(
                                alias=None,
                                function_name="mapContains",
                                parameters=(
                                    subscriptable_ref.column,
                                    subscriptable_ref.key,
                                ),
                            ),
                        ),
                    )
            elif isinstance(exp, CurriedFunctionCall):
                if (
                    exp.internal_function.function_name
                    in self._curried_aggregation_names
                ):
                    subscriptable_ref = find_subscriptable_reference(exp)
                    if subscriptable_ref:
                        return CurriedFunctionCall(
                            alias=exp.alias,
                            internal_function=FunctionCall(
                                alias=None,
                                function_name=f"{exp.internal_function.function_name}If",
                                parameters=exp.internal_function.parameters,
                            ),
                            parameters=(
                                *exp.parameters,
                                FunctionCall(
                                    alias=None,
                                    function_name="mapContains",
                                    parameters=(
                                        subscriptable_ref.column,
                                        subscriptable_ref.key,
                                    ),
                                ),
                            ),
                        )

            return exp

        query.transform_expressions(transform_aggregates_to_conditionals)
