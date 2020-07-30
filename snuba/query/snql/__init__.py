from parsimonious.grammar import Grammar
from snuba.datasets.dataset import Dataset
from snuba.query.logical import Query


grammar = Grammar(
    r"""
    query_exp = match_clause where_clause? collect_clause? having_clause? order_by_clause?
    match_clause = space* "MATCH" "(" clause ")" space*
    where_clause = space* "WHERE" clause space*
    collect_clause = space* "COLLECT" clause "BY" clause space*
    having_clause = space* "HAVING" clause space*
    order_by_clause = space* "ORDER BY" clause ("ASC"/"DESC") space*

    clause = space* ~r"[-=><\w]+" space*
    space = " "
"""
)


def parse_snql_query(body: str, dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """

    return Query({}, None,)
