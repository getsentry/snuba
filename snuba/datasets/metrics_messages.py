from enum import Enum
from typing import Any, Iterable, Mapping, MutableMapping

from snuba.state import get_config


class InputType(Enum):
    SET = "s"
    COUNTER = "c"
    DISTRIBUTION = "d"


class OutputType(Enum):
    SET = "set"
    COUNTER = "counter"
    DIST = "distribution"


class AggregationOption(Enum):
    HIST = "hist"
    TEN_SECOND = "ten_second"


ILLEGAL_VALUE_IN_SET = "Illegal value in set."
INT_EXPECTED = "Int expected"
ILLEGAL_VALUE_IN_DIST = "Illegal value in distribution."
ILLEGAL_VALUE_IN_COUNTER = "Illegal value in counter."
INT_FLOAT_EXPECTED = "Int or Float expected"

# These are the hardcoded values from the materialized view
GRANULARITY_TEN_SECONDS = 0
GRANULARITY_ONE_MINUTE = 1
GRANULARITY_ONE_HOUR = 2
GRANULARITY_ONE_DAY = 3


def is_set_message(message: Mapping[str, Any]) -> bool:
    return message["type"] is not None and message["type"] == InputType.SET.value


def is_distribution_message(message: Mapping[str, Any]) -> bool:
    return (
        message["type"] is not None and message["type"] == InputType.DISTRIBUTION.value
    )


def is_counter_message(message: Mapping[str, Any]) -> bool:
    return message["type"] is not None and message["type"] == InputType.COUNTER.value


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


def value_for_counter_message(message: Mapping[str, Any]) -> Mapping[str, Any]:
    value = message["value"]
    assert isinstance(
        value, (int, float)
    ), f"{ILLEGAL_VALUE_IN_COUNTER} {INT_FLOAT_EXPECTED}: {value}"

    return {"metric_type": OutputType.COUNTER.value, "count_value": value}


def apply_aggregation_option(
    settings: MutableMapping[str, Any], option: AggregationOption
) -> None:
    if option is AggregationOption.TEN_SECOND:
        settings["granularities"].append(GRANULARITY_TEN_SECONDS)
    elif option is AggregationOption.HIST:
        settings["enable_histogram"] = 1


def aggregation_options_for_set_message(
    message: Mapping[str, Any], retention_days: int
) -> Mapping[str, Any]:
    settings = {
        "granularities": [
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ],
        "min_retention_days": retention_days,
        "materialization_version": 2,
    }

    if aggregation_setting := message.get("aggregation_option"):
        parsed_aggregation_setting = AggregationOption(aggregation_setting)
        apply_aggregation_option(settings, parsed_aggregation_setting)

    return settings


def aggregation_options_for_distribution_message(
    message: Mapping[str, Any], retention_days: int
) -> Mapping[str, Any]:
    settings = {
        "granularities": [
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ],
        "min_retention_days": retention_days,
        "materialization_version": 2,
    }

    if aggregation_setting := message.get("aggregation_option"):
        parsed_aggregation_setting = AggregationOption(aggregation_setting)
        apply_aggregation_option(settings, parsed_aggregation_setting)

    return settings


def aggregation_options_for_counter_message(
    message: Mapping[str, Any], retention_days: int
) -> Mapping[str, Any]:
    settings = {
        "granularities": [
            GRANULARITY_ONE_MINUTE,
            GRANULARITY_ONE_HOUR,
            GRANULARITY_ONE_DAY,
        ],
        "min_retention_days": retention_days,
        "materialization_version": get_config("gen_metric_counters_mv_ver", 1),
    }

    if aggregation_setting := message.get("aggregation_option"):
        parsed_aggregation_setting = AggregationOption(aggregation_setting)
        apply_aggregation_option(settings, parsed_aggregation_setting)

    return settings
