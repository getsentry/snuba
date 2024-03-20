import logging

from snuba import environment
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.state import get_int_config
from snuba.utils.metrics.wrapper import MetricsWrapper

"""
Domain maps from a property to the specific values that are being filtered for. Ex:
    sumIf(value, metric_id in [1,2,3])
        Domain = {metric_id: {1,2,3}}
    sumIf(value, metric_id=5 and status=200) / sumIf(value, metric_id=5 and status=400)
        Domain = {
            metric_id: {5},
            status: {200, 400}
        }
"""
Domain = dict[Column | SubscriptableReference, set[Literal]]

metrics = MetricsWrapper(environment.metrics, "api")

logger = logging.getLogger(__name__)


class FilterInSelectOptimizer:
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

    def process_mql_query(
        self, query: LogicalQuery | CompositeQuery[QueryEntity]
    ) -> None:
        feat_flag = get_int_config("enable_filter_in_select_optimizer", default=0)
        if feat_flag:
            try:
                domain = self.get_domain_of_mql_query(query)
            except ValueError:
                logger.warning(
                    "Failed getting domain", exc_info=True, extra={"query": query}
                )
                domain = {}

            if domain:
                # add domain to where clause
                domain_filter = None
                for key, value in domain.items():
                    clause = binary_condition(
                        "in",
                        key,
                        FunctionCall(
                            alias=None,
                            function_name="array",
                            parameters=tuple(value),
                        ),
                    )
                    if not domain_filter:
                        domain_filter = clause
                    else:
                        domain_filter = binary_condition(
                            "and",
                            domain_filter,
                            clause,
                        )
                assert domain_filter is not None
                query.add_condition_to_ast(domain_filter)
                metrics.increment("kyles_optimizer_optimized")

    def get_domain_of_mql_query(
        self, query: LogicalQuery | CompositeQuery[QueryEntity]
    ) -> Domain:
        """
        This function returns the metric_id domain of the given query.
        For a definition of metric_id domain, go to definition of the return type of this function ('Domain')
        """
        expressions = map(lambda x: x.expression, query.get_selected_columns())
        target_exp = None
        for exp in expressions:
            if self._contains_conditional_aggregate(exp):
                if target_exp is not None:
                    raise ValueError(
                        "Was expecting only 1 select expression to contain condition aggregate but found multiple"
                    )
                else:
                    target_exp = exp

        if target_exp is not None:
            domains = self._get_conditional_domains(target_exp)
            if len(domains) == 0:
                raise ValueError("This shouldnt happen bc there is a target_exp")

            # find the intersect of keys, across the domains of all conditional aggregates
            key_intersect = set(domains[0].keys())
            for i in range(1, len(domains)):
                domain = domains[i]
                key_intersect = key_intersect.intersection(set(domains[i].keys()))

            # union the domains
            domain_union: Domain = {}
            for key in key_intersect:
                domain_union[key] = set()
                for domain in domains:
                    domain_union[key] = domain_union[key].union(domain[key])

            return domain_union
        else:
            return {}

    def _contains_conditional_aggregate(self, exp: Expression) -> bool:
        if isinstance(exp, FunctionCall):
            if exp.function_name[-2:] == "If":
                return True
            for param in exp.parameters:
                if self._contains_conditional_aggregate(param):
                    return True
            return False
        elif isinstance(exp, CurriedFunctionCall):
            if exp.internal_function.function_name[-2:] == "If":
                return True
            return False
        else:
            return False

    def _get_conditional_domains(self, exp: Expression) -> list[Domain]:
        domains: list[Domain] = []
        self._get_conditional_domains_helper(exp, domains)
        return domains

    def _get_conditional_domains_helper(
        self, exp: Expression, domains: list[Domain]
    ) -> None:
        if isinstance(exp, FunctionCall):
            # add domain of function call
            if exp.function_name[-2:] == "If":
                if len(exp.parameters) != 2 or not isinstance(
                    exp.parameters[1], FunctionCall
                ):
                    raise ValueError("unexpected form of function aggregate")
                domains.append(self._get_domain_of_predicate(exp.parameters[1]))
            else:
                for param in exp.parameters:
                    self._get_conditional_domains_helper(param, domains)
        elif isinstance(exp, CurriedFunctionCall):
            # add domain of curried function
            if exp.internal_function.function_name[-2:] == "If":
                if len(exp.parameters) != 2 or not isinstance(
                    exp.parameters[1], FunctionCall
                ):
                    raise ValueError("unexpected form of curried function aggregate")

                domains.append(self._get_domain_of_predicate(exp.parameters[1]))

    def _get_domain_of_predicate(self, p: FunctionCall) -> Domain:
        domain: Domain = {}
        self._get_domain_of_predicate_helper(p, domain)
        return domain

    def _get_domain_of_predicate_helper(
        self,
        p: FunctionCall,
        domain: Domain,
    ) -> None:
        if p.function_name == "equals":
            # validate
            if not len(p.parameters) == 2:
                raise ValueError("unexpected form of 'equals' function in predicate")
            lhs = p.parameters[0]
            rhs = p.parameters[1]
            if not isinstance(lhs, (Column, SubscriptableReference)) or not isinstance(
                rhs, Literal
            ):
                raise ValueError("unexpected form of 'equals' function in predicate")
            # if already there throw error, this was to protect against: and(field=1, field=2)
            if lhs in domain:
                raise ValueError("lhs of 'equals' was already seen (likely from and)")

            # add it to domain
            domain[lhs] = {rhs}
        elif p.function_name == "in":
            # validate
            if not len(p.parameters) == 2:
                raise ValueError("unexpected form of 'in' function in predicate")
            lhs = p.parameters[0]
            rhs = p.parameters[1]
            if not (
                isinstance(lhs, (Column, SubscriptableReference))
                and isinstance(rhs, FunctionCall)
                and rhs.function_name in ("array", "tuple")
            ):
                raise ValueError("unexpected form of 'in' function in predicate")
            # if already there throw error, this was to protect against: and(field=1, field=2)
            if lhs in domain:
                raise ValueError("lhs of 'in' was already seen (likely from and)")

            # add it to domain
            values = set()
            for e in rhs.parameters:
                if not isinstance(e, Literal):
                    raise ValueError(
                        "expected rhs of 'in' to only contain Literal, but that was not the case"
                    )
                values.add(e)
            domain[lhs] = values
        elif p.function_name == "and":
            if not (
                len(p.parameters) == 2
                and isinstance(p.parameters[0], FunctionCall)
                and isinstance(p.parameters[1], FunctionCall)
            ):
                raise ValueError("unexpected form of 'and' function in predicate")
            self._get_domain_of_predicate_helper(p.parameters[0], domain)
            self._get_domain_of_predicate_helper(p.parameters[1], domain)
        else:
            raise ValueError("unexpected form of predicate")
