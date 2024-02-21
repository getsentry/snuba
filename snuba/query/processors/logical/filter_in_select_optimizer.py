from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import StringWithSuffix
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class FilterInSelectOptimizer(LogicalQueryProcessor):
    """Transforms the expression avg(value) -> sum(value) / count(value)

    This processor was introduced for the gauges entity which has sum and count fields
    but not avg. This processor provides syntactic sugar for the product to be able to avg gauges.

    """

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        metric_id_filter = self.get_domain_of_query(query)
        if metric_id_filter is not None:
            query.add_condition_to_ast(metric_id_filter)

    def get_domain_of_query(self, query: Query) -> FunctionCall | None:
        """
        Returns the metric_id domain of the given query, in the form of a filter expression i.e. in(metric_id, [1,2,3]).
        Returns None if no filtering can be done.

        The metric_id domain of a query is the set of all metric_id that are relevant to this query, thus cannot be filtered out.
        And by this logic, any metric_id not in the metric_id domain of a query can be filtered out in the where clause.
        """
        conditional_aggregate_function_matcher = FunctionCallMatch(
            function_name=StringWithSuffix("If")
        )
        domain = None
        for select_exp in query.get_selected_columns():
            exp = select_exp.expression
            if conditional_aggregate_function_matcher.match(exp):
                assert isinstance(exp, FunctionCall)  # F in the chat for dynamic typing
                predicate = exp.parameters[1]
                assert isinstance(predicate, FunctionCall)
                predicate_domain = self._get_domain_of_predicate(predicate)
                if domain is None:
                    # this means its uninitialized
                    domain = predicate_domain
                else:
                    domain = self._domain_union(domain, predicate_domain)
                if domain is None:
                    # but now it means no filtering can occur, sorry for my confusing code
                    return None
        return domain

    def _get_domain_of_predicate(self, e: FunctionCall) -> FunctionCall | None:
        """
        Returns the metric_id domain of the given expression, in the form of a filter expression i.e. in(metric_id, [1,2,3]).
        Returns None if no filtering can be done.

        Requires e is a predicate, i.e. a logical true / false expression. (It would probably be good to have this enforced in the type system, but I am only human)

        The metric_id domain of a predicate is the set of all metric_id that the predicate could hold true with.
        ex: domain(tag==200) = set of all metric id
            domain(metric_id = 1) = {1}
        see https://www.notion.so/sentry/Optimizing-if-expressions-in-MQL-formula-queries-173105378b734fea925589fc9fb5817c?pvs=4 for more info
        """
        if e.function_name == "and":
            return self._get_domain_of_and(e)
        elif e.function_name == "or":
            return self._get_domain_of_or(e)
        elif e.function_name == "not":
            return self._get_domain_of_not(e)
        elif e.function_name == "equals":
            return self._get_domain_of_equals(e)
        elif e.function_name == "in":
            return self._get_domain_of_in(e)
        else:
            raise ValueError("this shouldnt happen")

    def _get_domain_of_and(self, e: FunctionCall) -> FunctionCall | None:
        if e.function_name != "and":
            raise ValueError("e must be 'and' function")
        assert isinstance(e.parameters[0], FunctionCall)
        assert isinstance(e.parameters[1], FunctionCall)
        lhs_domain = self._get_domain_of_predicate(e.parameters[0])
        rhs_domain = self._get_domain_of_predicate(e.parameters[1])
        return self._domain_intersect(lhs_domain, rhs_domain)

    def _get_domain_of_or(self, e: FunctionCall) -> FunctionCall | None:
        if e.function_name != "or":
            raise ValueError("e must be 'or' function")
        assert isinstance(e.parameters[0], FunctionCall)
        assert isinstance(e.parameters[1], FunctionCall)
        lhs_domain = self._get_domain_of_predicate(e.parameters[0])
        rhs_domain = self._get_domain_of_predicate(e.parameters[1])
        return self._domain_union(lhs_domain, rhs_domain)

    def _get_domain_of_not(self, e: FunctionCall) -> FunctionCall | None:
        if e.function_name != "not":
            raise ValueError("e must be 'not' function")
        p = e.parameters[0]
        assert isinstance(p, FunctionCall)
        if p.function_name == "and" or p.function_name == "or":
            # I only know how to work with nots at the level of in/equals
            return self._get_domain_of_predicate(self._demorgan(p))
        elif p.function_name == "not":
            assert isinstance(p.parameters[0], FunctionCall)
            return self._get_domain_of_predicate(p.parameters[0])
        elif p.function_name == "equals":
            assert isinstance(p.parameters[0], FunctionCall)
            eq_domain = self._get_domain_of_equals(p.parameters[0])
            if eq_domain is not None:
                return self._domain_complement(eq_domain)
            return None
        elif p.function_name == "in":
            assert isinstance(p.parameters[0], FunctionCall)
            in_domain = self._get_domain_of_in(p.parameters[0])
            if in_domain is not None:
                return self._domain_complement(in_domain)
            return None
        else:
            raise ValueError("this shouldnt happen")

    def _get_domain_of_equals(self, e: FunctionCall) -> FunctionCall | None:
        if e.function_name != "equals":
            raise ValueError("e must be 'equals' function")
        if (
            isinstance(e.parameters[0], Column)
            and e.parameters[0].column_name == "metric_id"
        ):
            return e
        return None

    def _get_domain_of_in(self, e: FunctionCall) -> FunctionCall | None:
        if e.function_name != "in":
            raise ValueError("e must be 'in' function")
        if (
            isinstance(e.parameters[0], Column)
            and e.parameters[0].column_name == "metric_id"
        ):
            return e
        return None

    def _demorgan(self, e: FunctionCall) -> FunctionCall:
        """given 'not(A and B)' or 'not(A or B)' returns an exxpression with the not distributed using demorgans theorem"""
        if e.function_name != "and" and e.function_name != "or":
            raise ValueError("must be and or or")
        if e.function_name == "and":
            new_fn = "or"
        else:
            new_fn = "and"
        return FunctionCall(
            alias=None,
            function_name=new_fn,
            parameters=(
                FunctionCall(
                    alias=None, function_name="not", parameters=(e.parameters[0],)
                ),
                FunctionCall(
                    alias=None, function_name="not", parameters=(e.parameters[1],)
                ),
            ),
        )

    def _domain_intersect(
        self, e1: FunctionCall | None, e2: FunctionCall | None
    ) -> FunctionCall | None:
        """Given 2 domains, returns the union"""
        if e1 is None and e2 is None:
            return None
        elif e1 is None and e2 is not None:
            return e1
        elif e2 is None and e1 is not None:
            return e2
        else:
            assert e1 is not None and e2 is not None  # mypy sucks
            return FunctionCall(
                alias=None,
                function_name="and",
                parameters=(e1, e2),
            )

    def _domain_union(
        self, e1: FunctionCall | None, e2: FunctionCall | None
    ) -> FunctionCall | None:
        """Given 2 domains, returns the union"""
        if e1 is None or e2 is None:
            return None
        else:
            return FunctionCall(
                alias=None,
                function_name="or",
                parameters=(e1, e2),
            )

    def _domain_complement(self, e1: FunctionCall) -> FunctionCall:
        """
        Given a domain, returns its compliment i.e. in(metric_id, [1,2,3]) becomes not(in(metric_id, [1,2,3]))
        """
        return FunctionCall(
            alias=None,
            function_name="not",
            parameters=(e1,),
        )
