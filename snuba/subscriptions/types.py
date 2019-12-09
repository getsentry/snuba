from typing import Any, TypeVar

from snuba.utils.types import Comparable


TTimestamp = TypeVar("TTimestamp", bound=Comparable[Any])
