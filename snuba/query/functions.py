# This is supposed to enumerate the functions snuba supports (with their
# validator) so we can keep control of the clickhouse functions snuba
# exposes.
#
# At this point it is just listing some of them used during query
# processing, so we can keep the list in one place only.

# Please keep them sorted alphabetically in two groups:
# Standard and Clickhouse specific.
_AGGREGATION_FUNCTIONS_BASE = {
    # Base
    "count",
    "min",
    "max",
    "sum",
    "avg",
    "any",
    "stddevPop",
    "stddevSamp",
    "varPop",
    "varSamp",
    "covarPop",
    "covarSamp",
    # Clickhouse Specific
    "anyHeavy",
    "anyLast",
    "argMin",
    "argMax",
    "avgWeighted",
    "corr",
    "topK",
    "topKWeighted",
    "groupArray",
    "groupUniqArray",
    "groupArrayInsertAt",
    "groupArrayMovingAvg",
    "groupArrayMovingSum",
    "groupBitAnd",
    "groupBitOr",
    "groupBitXor",
    "groupBitmap",
    "groupBitmapAnd",
    "groupBitmapOr",
    "groupBitmapXor",
    "sumWithOverflow",
    "sumMap",
    "minMap",
    "maxMap",
    "skewSamp",
    "skewPop",
    "kurtSamp",
    "kurtPop",
    "uniq",
    "uniqExact",
    "uniqCombined",
    "uniqCombined64",
    "uniqHLL12",
    "quantile",
    "quantiles",
    "quantileExact",
    "quantileExactLow",
    "quantileExactHigh",
    "quantileExactWeighted",
    "quantileTiming",
    "quantileTimingWeighted",
    "quantileDeterministic",
    "quantileTDigest",
    "quantileTDigestWeighted",
    "simpleLinearRegression",
    "stochasticLinearRegression",
    "stochasticLogisticRegression",
    "categoricalInformationValue",
    # Parametric
    "histogram",
    "sequenceMatch",
    "sequenceCount",
    "windowFunnel",
    "retention",
    "uniqUpTo",
    "sumMapFiltered",
}

_AGGREGATION_SUFFIXES = {
    "",
    "If",
    "Array",
    "SampleState",
    "State",
    "Merge",
    "MergeIf",
    "MergeState",
    "ForEach",
    "OrDefault",
    "OrNull",
    "Resample",
}

AGGREGATION_FUNCTIONS = {
    f"{f_name}{suffix}"
    for f_name in _AGGREGATION_FUNCTIONS_BASE
    for suffix in _AGGREGATION_SUFFIXES
}


def is_aggregation_function(func_name: str) -> bool:
    return func_name in AGGREGATION_FUNCTIONS


# Categorized based on ClickHouse docs
# https://clickhouse.tech/docs/en/sql-reference/functions/
REGULAR_FUNCTIONS = {
    # arithmetic
    "abs",
    "plus",
    "minus",
    "multiply",
    "divide",
    # arrays,
    "array",
    "arrayConcat",
    "arrayElement",
    "arrayExists",
    "arrayAll",
    "indexOf",
    "has",
    "hasAny",
    "notEmpty",  # can apply to strings as well
    "length",  # can apply to strings as well
    # comparison
    "equals",
    "notEquals",
    "less",
    "greater",
    "lessOrEquals",
    "greaterOrEquals",
    # logical
    "and",
    "or",
    "not",
    # type comparison
    "toDateTime",
    "toString",
    "toInt8",
    "toUInt8",
    "toUInt64",
    "toFloat64",
    # dates and times
    "toStartOfMinute",
    "toStartOfDay",
    "toStartOfHour",
    "toStartOfYear",
    "toUnixTimestamp",
    # conditionals
    "if",
    "multiIf",
    # mathematical
    "log",
    "sqrt",
    # rounding functions
    "floor",
    # hash functions
    "cityHash64",
    # functions for IN operator
    "in",
    "notIn",
    # functions for searching strings
    "like",
    "notLike",
    "match",
    "positionCaseInsensitive",
    "multiSearchFirstPositionCaseInsensitive",
    # functions for nulls
    "isNull",
    "isNotNull",
    "ifNull",
    "assumeNotNull",
    "coalesce",
    # functions for tuples
    "tuple",
    "tupleElement",
    # other,
    "transform",
    "least",
    "greatest",
    # table functions
    "arrayJoin",
}

# Custom function names that are not in ClickHouse but need to be validated
CUSTOM_FUNCTIONS = {
    # transactions
    "apdex",
    "failure_rate",
    # events
    "isHandled",
    "notHandled",
}

# Functions that take lambdas as arguments
HIGHER_ORDER_FUNCTIONS = {
    "arrayCount",
    "arraySort",
    "arrayReverseSort",
    "arrayMap",
    "arrayFilter",
    "arrayFill",
    "arrayReverseFill",
    "arraySplit",
    "arrayReverseSplit",
    "arrayExists",
    "arrayAll",
    "arrayFirst",
    "arrayLast",
    "arrayFirstIndex",
    "arrayLastIndex",
    "arrayMin",
    "arrayMax",
    "arraySum",
    "arrayAvg",
    "arrayCumSum",
    "arraySumNonNegative",
}

GLOBAL_VALID_FUNCTIONS = (
    set() | REGULAR_FUNCTIONS | AGGREGATION_FUNCTIONS | CUSTOM_FUNCTIONS | HIGHER_ORDER_FUNCTIONS
)


def is_valid_global_function(func_name: str) -> bool:
    return func_name in GLOBAL_VALID_FUNCTIONS
