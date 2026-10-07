from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

ConditionsType = Mapping[str, Sequence[str | int | float]]


@dataclass
class AttributeConditions:
    item_type: TraceItemType.ValueType
    attributes: dict[str, tuple[AttributeKey, list[Any]]]


@dataclass
class ConditionsBag:
    column_conditions: ConditionsType
    attribute_conditions: AttributeConditions | None = None
