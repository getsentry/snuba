from enum import Enum
from typing import Any, Mapping


class OutputType(Enum):
    SET = "set"
    COUNTER = "counter"
    DIST = "distribution"


ILLEGAL_VALUE_IN_SET = "Illegal value in set."
INT_EXPECTED = "Int expected"


def is_set_message(message: Mapping[str, Any]) -> bool:
    return message["type"] is not None and message["type"] == "s"


def values_for_set_message(message: Mapping[str, Any]) -> Mapping[str, Any]:
    values = message["value"]
    for value in values:
        assert isinstance(value, int), f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
    return {"metric_type": OutputType.SET.value, "set_values": values}
