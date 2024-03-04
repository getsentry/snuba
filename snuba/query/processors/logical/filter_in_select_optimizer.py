from snuba.query.conditions import binary_condition
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_int_config


class FilterInSelectOptimizer(LogicalQueryProcessor):
    """
    This optimizer takes queries that filter by metric_id in the select clause (via conditional aggregate functions),
    and adds the equivalent metric_id filtering to the where clause. Example:

        SELECT sumIf(value, metric_id in (1,2,3,4))
        FROM table

        becomes

        SELECT sumIf(value, metric_id in (1,2,3,4))
        FROM table
        WHERE metric_id in (1,2,3,4)
    """

    """
    Definition of 'metric_id domain': The set of all metric_id that are relevant to a query.
        Said another way: the set of all metric_id that are being filtered.

        Heres some examples:
        * SELECT sumIf(value, metric_id in (1,2,3,4)) has a metric_id domain = {1,2,3,4}
        * SELECT add(sumIf(value, metric_id in (1,2,3,4)), sumIf(value, metric_id in (5,6,7,8)))
            has a metric_id domain = {1,2,3,4,5,6,7,8}

        I also use this to refer to conditional filters as well, so:
        * equals(metric_id, 8) has metric_id domain = {8}
        * in(metric_id, [7, 11, 13]) has metric_id domain = {7, 11, 13}

        Additionally, a metric_id domain of None is equivalent to the set of all metric_id.
        * equals(status, 200) has metric_id domain = None
            why? because this condition can hold true for any metric_id
    """
    Domain = set[int] | None

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        feat_flag = get_int_config("enable_filter_in_select_optimizer", default=0)
        if feat_flag:
            domain = self.get_domain_of_query(query)
            if domain is not None:
                filter = binary_condition(
                    "in",
                    Column(None, None, "metric_id"),
                    FunctionCall(
                        alias=None,
                        function_name="array",
                        parameters=tuple(map(lambda x: Literal(None, x), domain)),
                    ),
                )
                query.add_condition_to_ast(filter)

    def get_domain_of_query(self, query: Query) -> Domain:
        """
        This function returns the metric_id domain of the given query.
        For a definition of metric_id domain, go to definition of the return type of this function ('Domain')
        """
        for select_exp in query.get_selected_columns():
            curr = self._get_domain_of_exp(select_exp.expression)
            if curr is not None:
                return curr
        return None

    def _get_domain_of_exp(self, exp: Expression) -> Domain:
        if isinstance(exp, FunctionCall):
            return self._get_domain_of_function_call(exp)
        elif isinstance(exp, CurriedFunctionCall):
            return self._get_domain_of_curried_function_call(exp)
        elif isinstance(exp, Literal):
            return set()
        else:
            # we dont know so we just say its None
            return None

    def _get_domain_of_function_call(self, f: FunctionCall) -> Domain:
        """
        The domain of a function call is either:
            - case 1: its a conditional aggregate, so the domain of that
            - case 2: any other function is the union of domains of its params
                ex. sumIf1 + sumIf2 / sumIf3
        """
        if f.function_name[-2:] == "If":
            if len(f.parameters) != 2 or not isinstance(f.parameters[1], FunctionCall):
                # If the conditional function is not in the form functionIf(value, condition), I bail
                return None

            return self._get_domain_of_predicate(f.parameters[1])
        else:
            # this section assumes that the domain of a generic function is
            # the union of its parameters' domain.
            # for sum(), divide(), etc this hold true but im not sure
            # if its bad to make such a general assumption

            # return the union of the domains of all parameters
            if len(f.parameters) == 0:
                return None

            domain = self._get_domain_of_exp(f.parameters[0])
            for i in range(1, len(f.parameters)):
                curr = self._get_domain_of_exp(f.parameters[i])
                if domain is None or curr is None:
                    return None
                else:
                    domain = domain.union(curr)
            return domain

    def _get_domain_of_curried_function_call(self, f: CurriedFunctionCall) -> Domain:
        if f.internal_function.function_name[-2:] == "If":
            assert isinstance(f.parameters[1], FunctionCall)
            return self._get_domain_of_predicate(f.parameters[1])
        else:
            return None

    def _get_domain_of_predicate(self, predicate: FunctionCall) -> set[int] | None:
        if self._is_metric_id_condition(predicate):
            return self._get_domain_of_metric_id_condition(predicate)
        elif predicate.function_name == "and":
            return self._get_domain_of_and(predicate)
        else:
            return None

    def _get_domain_of_equals(self, f: FunctionCall) -> set[int] | None:
        if f.function_name != "equals":
            raise ValueError("Given function must be 'equals")

        if (
            len(f.parameters) == 2
            and isinstance(f.parameters[0], Column)
            and f.parameters[0].column_name == "metric_id"
            and isinstance(f.parameters[1], Literal)
            and isinstance(f.parameters[1].value, int)
        ):
            return {f.parameters[1].value}
        else:
            return None

    def _get_domain_of_in(self, f: FunctionCall) -> set[int] | None:
        if f.function_name != "in":
            raise ValueError("Given function must be 'in")

        if (
            len(f.parameters) == 2
            and isinstance(f.parameters[0], Column)
            and f.parameters[0].column_name == "metric_id"
            and isinstance(f.parameters[1], FunctionCall)
            and f.parameters[1].function_name == "array"
        ):
            rhs = f.parameters[1]
            ids = set()
            for metric_id in rhs.parameters:
                if isinstance(metric_id, Literal) and isinstance(metric_id.value, int):
                    ids.add(metric_id.value)
                else:
                    return None
            return ids
        else:
            return None

    def _get_domain_of_and(self, f: FunctionCall) -> Domain:
        if f.function_name != "and":
            raise ValueError("Given function must be 'and")

        if len(f.parameters) != 2:
            return None
        lhs = f.parameters[0]
        rhs = f.parameters[1]
        if self._is_metric_id_condition(lhs) and not self._contains_metric_id_condition(
            rhs
        ):
            assert isinstance(lhs, FunctionCall)
            return self._get_domain_of_metric_id_condition(lhs)
        elif self._is_metric_id_condition(
            rhs
        ) and not self._contains_metric_id_condition(lhs):
            assert isinstance(rhs, FunctionCall)
            return self._get_domain_of_metric_id_condition(rhs)
        else:
            return None

    def _get_domain_of_metric_id_condition(self, f: FunctionCall) -> Domain:
        if not self._is_metric_id_condition(f):
            raise ValueError("Must be metric_id condition")

        if f.function_name == "equals":
            return self._get_domain_of_equals(f)
        else:
            return self._get_domain_of_in(f)

    def _is_metric_id_condition(self, f: Expression) -> bool:
        """
        A metric_id condition is either:
          * equals(metric_id, ...)
          * in(metric_id, ...)
        """
        return (
            isinstance(f, FunctionCall)
            and f.function_name in ("equals", "in")
            and isinstance(f.parameters[0], Column)
            and f.parameters[0].column_name == "metric_id"
        )

    def _contains_metric_id_condition(self, f: Expression) -> bool:
        """
        Returns true if the given FunctionCall contains any filtering by metric_id
        """
        if not isinstance(f, FunctionCall):
            return False
        elif self._is_metric_id_condition(f):
            return True
        else:
            for p in f.parameters:
                if self._contains_metric_id_condition(p):
                    return True
            return False
