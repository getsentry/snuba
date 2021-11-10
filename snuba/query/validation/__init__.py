from abc import ABC, abstractmethod
from typing import Sequence

from snuba.query.data_source import DataSource
from snuba.query.expressions import Expression
from snuba.utils.serializable_exception import SerializableException


class InvalidFunctionCall(SerializableException):
    pass


class FunctionCallValidator(ABC):
    """
    Validates the signature of a function call given the parameters
    and the schema of the function call.
    Raise InvalidFunctionCall to signal an invalid call.
    """

    @abstractmethod
    def validate(
        self, func_name: str, parameters: Sequence[Expression], data_source: DataSource
    ) -> None:
        raise NotImplementedError
