from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

ConditionsType = Mapping[str, Sequence[str | int | float]]


@dataclass
class AttributeConditions:
    item_type: int
    attributes: Dict[str, Tuple[AttributeKey, List[Any]]]


@dataclass
class ConditionsBag:
    column_conditions: ConditionsType
    attribute_conditions: Optional[AttributeConditions] = None
