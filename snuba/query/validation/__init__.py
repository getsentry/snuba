from abc import ABC, abstractmethod
from typing import Sequence

from snuba.clickhouse.columns import ColumnSet, SchemaModifiers
from snuba.query.expressions import Expression


class InvalidFunctionCall(Exception):
    pass


class FunctionCallValidator(ABC):
    """
    Validates the signature of a function call given the parameters
    and the schema of the function call.
    Raise InvalidFunctionCall to signal an invalid call.
    """

    @abstractmethod
    def validate(
        self, parameters: Sequence[Expression], schema: ColumnSet[SchemaModifiers]
    ) -> None:
        raise NotImplementedError
