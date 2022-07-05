from enum import Enum
from typing import Any, Iterable, Mapping


class InputType(Enum):
    SET = "s"
    COUNTER = "c"
    DISTRIBUTION = "d"


class OutputType(Enum):
    SET = "set"
    COUNTER = "counter"
    DIST = "distribution"


ILLEGAL_VALUE_IN_SET = "Illegal value in set."
INT_EXPECTED = "Int expected"
ILLEGAL_VALUE_IN_DIST = "Illegal value in distribution."
INT_FLOAT_EXPECTED = "Int or Float expected"


def is_set_message(message: Mapping[str, Any]) -> bool:
    return message["type"] is not None and message["type"] == InputType.SET.value


def is_distribution_message(message: Mapping[str, Any]) -> bool:
    return (
        message["type"] is not None and message["type"] == InputType.DISTRIBUTION.value
    )


def values_for_set_message(message: Mapping[str, Any]) -> Mapping[str, Any]:
    values = message["value"]
    assert isinstance(values, Iterable), "expected iterable of values for set"
    for value in values:
        assert isinstance(value, int), f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
    return {"metric_type": OutputType.SET.value, "set_values": values}


def values_for_distribution_message(message: Mapping[str, Any]) -> Mapping[str, Any]:
    values = message["value"]
    assert isinstance(values, Iterable), "expected iterable of values for distribution"
    for value in values:
        assert isinstance(
            value, (int, float)
        ), f"{ILLEGAL_VALUE_IN_DIST} {INT_FLOAT_EXPECTED}: {value}"

    return {"metric_type": OutputType.DIST.value, "distribution_values": values}
