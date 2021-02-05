import re

from typing import Optional, Pattern

ESCAPE_STRING_RE = re.compile(r"(['\\])")
ESCAPE_COL_RE = re.compile(r"([`\\])")
NEGATE_RE = re.compile(r"^(-?)(.*)$")
SAFE_COL_RE = re.compile(r"^-?([a-zA-Z_][a-zA-Z0-9_\.]*)$")
# Alias escaping is different than column names when we introduce table aliases.
# Using the column escaping function would consider "." safe, which is not for
# an alias.
SAFE_ALIAS_RE = re.compile(r"^-?[a-zA-Z_][a-zA-Z0-9_]*$")


def escape_string(str: str) -> str:
    str = ESCAPE_STRING_RE.sub(r"\\\1", str)
    return "'{}'".format(str)


def escape_expression(expr: Optional[str], regex: Pattern[str]) -> Optional[str]:
    if not expr:
        return expr
    elif regex.match(expr):
        # Column/Alias is safe to use without wrapping.
        return expr
    else:
        # Column/Alias needs special characters escaped, and to be wrapped with
        # backticks. If the column starts with a '-', keep that outside the
        # backticks as it is not part of the column name, but used by the query
        # generator to signify the sort order if we are sorting by this column.
        col = ESCAPE_COL_RE.sub(r"\\\1", expr)
        negate_match = NEGATE_RE.match(col)
        assert negate_match is not None
        return "{}`{}`".format(*negate_match.groups())


def escape_alias(alias: Optional[str]) -> Optional[str]:
    return escape_expression(alias, SAFE_ALIAS_RE)


def escape_identifier(col: Optional[str]) -> Optional[str]:
    return escape_expression(col, SAFE_COL_RE)
