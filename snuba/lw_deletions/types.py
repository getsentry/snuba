from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey


@dataclass
class AttributeConditions:
    item_type: int
    attributes: Dict[str, List[Any]]
    attributes_by_key: Dict[str, Tuple[AttributeKey, List[Any]]]
