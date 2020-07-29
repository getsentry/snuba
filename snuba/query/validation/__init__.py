from typing import Sequence
from abc import ABC, abstractmethod

from snuba.clickhouse.columns import ColumnSet
from snuba.query.expressions import Expression


class FunctionCallValidator(ABC):
    @abstractmethod
    def validate(self, parameters: Sequence[Expression], schema: ColumnSet) -> None:
        raise NotImplementedError
