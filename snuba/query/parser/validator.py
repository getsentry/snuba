import logging
from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.query.logical import Query
from snuba.query.parser.exceptions import ValidationException
from snuba.query.validation import ExpressionValidator
from snuba.query.validation.like import LikeFunctionValidator
from snuba.state import get_config

logger = logging.getLogger(__name__)

default_validators: Sequence[ExpressionValidator] = [
    LikeFunctionValidator(),
]


def validate_query(query: Query, dataset: Dataset) -> None:
    """
    Applies all the expression validators (default and dataset specific)
    in one pass over the AST.
    """

    validators = [*default_validators, *dataset.get_expression_validators()]
    schema = dataset.get_abstract_columnset()
    for exp in query.get_all_expressions():
        for v in validators:
            try:
                v.validate(exp, schema)
            except ValidationException as exception:
                if get_config("enforce_expression_validation", 0):
                    raise exception
                else:
                    logger.warning("Query validation exception", exc_info=True)
