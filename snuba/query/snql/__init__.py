from typing import Any, MutableMapping
from snuba.datasets.dataset import Dataset
from snuba.query.logical import Query


def parse_snql_query(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """

    return Query(body, None, None, None, None, None, None, None,)
