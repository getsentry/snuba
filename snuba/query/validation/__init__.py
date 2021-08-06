from abc import ABC, abstractmethod
from typing import Any, Sequence

<<<<<<< HEAD
from snuba.query.data_source import DataSource
=======
>>>>>>> 22c383ab (new AllowedFunctionValidator)
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
<<<<<<< HEAD
        self, parameters: Sequence[Expression], data_source: DataSource
=======
        self, func_name: str, parameters: Sequence[Expression], schema: Any
>>>>>>> 22c383ab (new AllowedFunctionValidator)
    ) -> None:
        raise NotImplementedError
