from abc import ABC, abstractmethod

from snuba.clickhouse.columns import ColumnSet
from snuba.query.expressions import Expression


class ExpressionValidator(ABC):
    """
    Validates an individual expression in a Snuba logical query.
    """

    @abstractmethod
    def validate(self, exp: Expression, schema: ColumnSet) -> None:
        """
        If the expression is valid according to this validator it
        returns, otherwise it raises a subclass of
        snuba.query.parser.ValidationException
        """
        raise NotImplementedError
