from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings

Domain = set[int] | None


class FilterInSelectOptimizer(LogicalQueryProcessor):
    """Transforms the expression avg(value) -> sum(value) / count(value)

    This processor was introduced for the gauges entity which has sum and count fields
    but not avg. This processor provides syntactic sugar for the product to be able to avg gauges.
    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        domain = self.get_domain_of_query(query)
        if domain is not None:
            domain_filter = binary_condition(
                "in",
                Column(None, None, "metric_id"),
                FunctionCall(
                    alias=None,
                    function_name="array",
                    parameters=tuple(map(lambda x: Literal(None, x), domain)),
                ),
            )

            query.add_condition_to_ast(domain_filter)

    def get_domain_of_query(self, query: Query) -> Domain:
        """
        Returns the metric_id domain of the given query, where the metric_id domain is defined as all metric_id that are relevant to the query.
        Returns None if the metric_id domain is all metric_id.

        If a metric_id is not in the domain of the query it can be filtered out.
        It is always acceptable to have the following clause in a query:
            WHERE metric_id in metric_id_domain
        """
        for select_exp in query.get_selected_columns():
            curr = self._get_domain_of_exp(select_exp.expression)
            if curr is not None:
                return curr
        return None

    def _get_domain_of_exp(self, exp: Expression) -> Domain:
        if isinstance(exp, FunctionCall):
            return self._get_domain_of_function_call(exp)
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
            return self._get_domain_of_cond_agg(f)
        else:
            if len(f.parameters) == 0:
                return None
            else:
                domain = self._get_domain_of_exp(f.parameters[0])
                for i in range(1, len(f.parameters)):
                    domain = self._domain_union(
                        domain, self._get_domain_of_exp(f.parameters[i])
                    )
                return domain

    def _get_domain_of_cond_agg(self, e: FunctionCall) -> set[int] | None:
        predicate = e.parameters[1]
        assert isinstance(predicate, FunctionCall)
        if predicate.function_name == "equals":
            return self._get_domain_of_equals(predicate)
        elif predicate.function_name == "in":
            return self._get_domain_of_in(predicate)
        elif predicate.function_name == "and":
            return self._get_domain_of_and(predicate)
        else:
            return None

    def _get_domain_of_equals(self, e: FunctionCall) -> set[int] | None:
        assert e.function_name == "equals"
        lhs = e.parameters[0]
        if isinstance(lhs, Column) and lhs.column_name == "metric_id":
            rhs = e.parameters[1]
            assert isinstance(rhs, Literal)
            assert isinstance(rhs.value, int)
            return {rhs.value}
        else:
            return None

    def _get_domain_of_in(self, e: FunctionCall) -> set[int] | None:
        assert e.function_name == "in"
        lhs = e.parameters[0]
        rhs = e.parameters[1]
        if isinstance(lhs, Column) and lhs.column_name == "metric_id":
            assert isinstance(rhs, FunctionCall) and rhs.function_name == "array"
            ids = set()
            for metric_id in rhs.parameters:
                assert isinstance(metric_id, Literal) and isinstance(
                    metric_id.value, int
                )
                ids.add(metric_id.value)
            return ids
        else:
            return None

    def _get_domain_of_and(self, e: FunctionCall) -> Domain:
        assert e.function_name == "and"
        lhs = e.parameters[0]
        rhs = e.parameters[1]
        assert isinstance(lhs, FunctionCall) and isinstance(rhs, FunctionCall)
        lres = self._is_mid_cond(lhs)
        rres = self._is_mid_cond(rhs)
        if lres == rres:
            # unsupported, or no filtering
            return None

        # 'and' must have metric_id condition on one side, and irrelivance on the other
        # in both of these cases the aggregate predicate is unsupported
        if lres and self._contains_midcond(rhs):
            return None
        elif rres and self._contains_midcond(lhs):
            return None

        condside = lhs if lres else rhs
        if condside.function_name == "equals":
            return self._get_domain_of_equals(condside)
        else:
            return self._get_domain_of_in(condside)

    def _domain_union(self, e1: Domain, e2: Domain) -> Domain:
        """Given 2 domains, returns the union"""
        if e1 is None or e2 is None:
            return None
        else:
            return e1.union(e2)

    def _is_mid_cond(self, f: FunctionCall) -> bool:
        if f.function_name != "equals" and f.function_name != "in":
            return False
        if not isinstance(f.parameters[0], Column):
            return False
        return f.parameters[0].column_name == "metric_id"

    def _contains_midcond(self, f: FunctionCall) -> bool:
        if self._is_mid_cond(f):
            return True
        for p in f.parameters:
            if isinstance(p, FunctionCall) and self._contains_midcond(p):
                return True
        return False
