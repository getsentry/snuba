from dataclasses import dataclass
from typing import Sequence, Union

# We define some dataclasses here to make it harder to accidentally
# pass unescaped user strings to ClickHouse in ValuesRowEncoder (though
# at the time of writing we send no user-generated text input to Clickhouse
# through ValuesRowEncoder)


@dataclass
class ClickhouseInt:
    val: int


@dataclass
class ClickhouseNumArray:
    """
    An array of either floats or ints
    """

    val: Union[Sequence[float], Sequence[int]]


@dataclass
class ClickhouseUnguardedExpression:
    """
    This is unescaped text that will be sent to Clickhouse and evaluated
    as an expression so we want to make it clear from naming
    that using this can be unsafe
    """

    val: str
