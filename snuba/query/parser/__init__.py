from typing import Any, MutableMapping

from snuba.query.expressions import Column, Expression, Literal
from snuba.datasets.dataset import Dataset
from snuba.query.parser.conditions import parse_conditions_to_expr
from snuba.query.parser.expressions import parse_expression
from snuba.query.parser.functions import parse_function_to_expr
from snuba.query.query import Query


def parse_query(body: MutableMapping[str, Any], dataset: Dataset,) -> Query:
    source = dataset.get_dataset_schemas().get_read_schema().get_data_source()
    return Query(body, source)
