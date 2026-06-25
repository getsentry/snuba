from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, TypedDict, cast

from snuba.reader import Column, Result, Row, transform_rows
from snuba.utils.serializable_exception import JsonSerializable, SerializableException


class QueryExtraData(TypedDict):
    stats: Dict[str, Any]
    sql: str
    experiments: Mapping[str, Any]


class QueryException(SerializableException):
    """
    Exception raised during query execution that is used to carry extra data
    back up the stack to the HTTP response -- basically a ``QueryResult``,
    but without an actual ``Result`` instance. This exception should always
    be chained with another exception that contains additional detail about
    the cause of the exception.
    """

    def __init__(
        self,
        exception_type: str | None = None,
        message: str | None = None,
        should_report: bool = True,
        **extra_data: JsonSerializable,
    ) -> None:
        self.exception_type = exception_type
        super().__init__(message, should_report, **extra_data)

    @classmethod
    def from_args(
        cls, exception_type: str, message: str, extra: QueryExtraData
    ) -> "QueryException":
        return cls(
            exception_type=exception_type,
            message=message,
            extra=cast(JsonSerializable, extra),
        )

    @property
    def extra(self) -> QueryExtraData:
        extra = self.extra_data.get("extra", None)
        if not extra:
            return QueryExtraData(stats={}, sql="noquery", experiments={})
        return cast(QueryExtraData, extra)


class QueryTooLongException(SerializableException):
    """
    Exception thrown when a query string is too long for ClickHouse.

    There is a limit for the maximum size of a query (in bytes)
    ClickHouse will process, this limit is defined in Snuba settings.
    """


@dataclass(frozen=True)
class QueryResult:
    result: Result
    extra: QueryExtraData

    @property
    def quota_allowance(self) -> Mapping[str, Mapping[str, Any]]:
        return self.extra.get("stats", {}).get("quota_allowance", {})


def transform_column_names(result: QueryResult, mapping: Mapping[str, list[str]]) -> None:
    """
    Replaces the column names in a ResultSet object in place.

    This runs on every query (including cache hits), so it avoids allocating a
    brand new dict for every row whenever the shape of the renaming allows it:

    * Identity mapping (every alias maps only to itself): the rows and meta are
      already correct, so we return immediately without touching the data.
    * Simple 1:1 rename with no colliding target names: the keys of each row are
      renamed in place, so no second dict is allocated per row.
    * Fan-out (one alias -> several names) or colliding targets: fall back to
      building a new dict per row, which is the only way to handle those safely.
    """
    # Collect the (alias -> name) pairs where the output name actually differs
    # from the alias, and detect whether any alias fans out to multiple names.
    renames: list[tuple[str, str]] = []
    has_fan_out = False
    for alias, names in mapping.items():
        if len(names) != 1:
            has_fan_out = True
            break
        if names[0] != alias:
            renames.append((alias, names[0]))

    if not has_fan_out:
        if not renames:
            # The mapping renames nothing: rows and meta are already correct.
            return

        targets = {new for _, new in renames}
        existing = {c["name"] for c in result.result["meta"]}
        # In-place renaming is safe (order-independent and never clobbers a value
        # that is still needed) only when every target name is unique and none of
        # them collides with a column name that already exists in the rows. A
        # collision would happen for a passthrough/identity column whose name is
        # reused as a target, or for a chained rename (a -> b, b -> c); in those
        # cases we fall back to rebuilding the row dicts, which is always correct.
        if len(targets) == len(renames) and targets.isdisjoint(existing):

            def rename_in_place(row: Row) -> Row:
                for old, new in renames:
                    if old in row:
                        row[new] = row.pop(old)
                return row

            transform_rows(result.result, rename_in_place)
            _transform_meta_names(result, mapping)
            return

    def transformer(row: Row) -> Row:
        new_row: Row = {}
        for key, value in row.items():
            column_names = mapping.get(key, [key])
            for c in column_names:
                new_row[c] = value
        return new_row

    transform_rows(result.result, transformer)
    _transform_meta_names(result, mapping)


def _transform_meta_names(result: QueryResult, mapping: Mapping[str, list[str]]) -> None:
    new_meta = []
    for c in result.result["meta"]:
        names = mapping.get(c["name"], [c["name"]])
        for n in names:
            new_meta.append(Column(name=n, type=c["type"]))
    result.result["meta"] = new_meta
