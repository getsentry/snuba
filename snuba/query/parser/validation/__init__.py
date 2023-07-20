from abc import ABC, abstractmethod
from typing import Sequence

from snuba.query import Query
from snuba.query.data_source import DataSource
from snuba.query.expressions import Expression


class ExpressionValidator(ABC):
    """
    Validates an individual expression in a Snuba logical query.
    """

    @abstractmethod
    def validate(self, exp: Expression, data_source: DataSource) -> None:
        """
        If the expression is valid according to this validator it
        returns, otherwise it raises a subclass of
        snuba.query.parser.ValidationException
        """
        raise NotImplementedError


from snuba.query.parser.validation.functions import FunctionCallsValidator

validators: Sequence[ExpressionValidator] = [FunctionCallsValidator()]


def validate_query(query: Query) -> None:
    """
    Applies all the expression validators in one pass over the AST.
    """

    for exp in query.get_all_expressions():
        for v in validators:
            v.validate(exp, query.get_from_clause())
