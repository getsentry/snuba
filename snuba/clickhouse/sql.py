"""Small SQL fragments that have to be escaped the same way everywhere.

Hand-built statements in the admin tools and the manual jobs kept re-deriving
these, and had drifted: the ON CLUSTER clause was written out in four places, two
of which interpolated the cluster name raw. Values belong in driver parameters
rather than here -- see ClickhousePool.execute(params=...) -- but identifiers and
clause keywords cannot be bound, so they still need building by hand.
"""

from collections.abc import Iterable

from snuba.clickhouse.escaping import escape_identifier, escape_string


def identifier(name: str) -> str:
    """escape_identifier that cannot return None, so callers stop unwrapping it."""
    escaped = escape_identifier(name)
    assert escaped is not None, "escape_identifier returns None only for a None name"
    return escaped


def literal_list(values: Iterable[str]) -> str:
    """Escaped, comma separated literals for an IN (...) list."""
    return ", ".join(escape_string(value) for value in values)


def on_cluster_clause(cluster_name: str | None) -> str:
    """` ON CLUSTER '<name>'`, or empty when there is no cluster.

    Includes its own leading space so callers can concatenate unconditionally.
    """
    if not cluster_name:
        return ""
    return f" ON CLUSTER {escape_string(cluster_name)}"
