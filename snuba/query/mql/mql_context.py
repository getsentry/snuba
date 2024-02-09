from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from snuba.query.parser.exceptions import ParsingException


@dataclass
class MQLContext:
    """
    The MQL string alone is not enough to fully describe a query.
    This class contains all of the additional information needed to
    execute a metrics query in snuba.

    This class is meant as a convenient way to validate that all of the
    required fields are present and of the correct type. It's not meant
    to build actual expressions. It should line up with the schema of
    the request itself. It should not do any validation beyond type checks.
    """

    entity: dict[str, str]
    start: str
    end: str
    rollup: Rollup
    scope: MetricsScope
    indexer_mappings: dict[str, str | int]
    limit: int | None
    offset: int | None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.entity, dict):
            raise ParsingException("MQL context: entity must be a dict")
        for field in ["start", "end"]:
            if not isinstance(getattr(self, field), str) or getattr(self, field) == "":
                raise ParsingException(f"MQL context: {field} must be a str")
        if not isinstance(self.rollup, Rollup):
            raise ParsingException("MQL context: rollup must be a Rollup")
        if not isinstance(self.scope, MetricsScope):
            raise ParsingException("MQL context: scope must be a MetricsScope")
        if not isinstance(self.indexer_mappings, dict):
            raise ParsingException("MQL context: indexer_mappings must be a dict")
        if self.limit is not None and not isinstance(self.limit, int):
            raise ParsingException("MQL context: limit must be an int")
        if self.offset is not None and not isinstance(self.offset, int):
            raise ParsingException("MQL context: offset must be an int")

    @staticmethod
    def from_dict(mql_context_dict: dict[str, Any]) -> MQLContext:
        try:
            return MQLContext(
                entity=mql_context_dict["entity"],
                start=mql_context_dict["start"],
                end=mql_context_dict["end"],
                rollup=Rollup(**mql_context_dict["rollup"]),
                scope=MetricsScope(**mql_context_dict["scope"]),
                indexer_mappings=mql_context_dict["indexer_mappings"],
                limit=mql_context_dict["limit"],
                offset=mql_context_dict["offset"],
            )
        except KeyError as e:
            raise ParsingException(f"MQL context: missing required field {e}")


@dataclass(frozen=True)
class Rollup:
    """
    Rollup instructs how the timeseries queries should be grouped on time. If the query is for a set of timeseries, then
    the interval field should be specified. It is the number of seconds to group the timeseries by.
    For a query that returns only the totals, specify Totals(True). A totals query can be ordered using the orderby field.
    If totals is set to True and the interval is specified, then an extra row will be returned in the result with the totals
    for the timeseries.
    """

    granularity: int
    interval: int | None = None
    with_totals: str | None = None
    orderby: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.granularity, int):
            raise ParsingException("MQL context: granularity must be an integer")
        if self.interval is not None and not isinstance(self.interval, int):
            raise ParsingException("MQL context: interval must be an integer")
        if self.with_totals is not None and not isinstance(self.with_totals, str):
            raise ParsingException("MQL context: totals must be a string")
        if self.orderby is not None and not isinstance(self.orderby, str):
            raise ParsingException("MQL context: orderby must be a string")


ALLOWED_GRANULARITIES = (10, 60, 3600, 86400)


@dataclass
class MetricsScope:
    org_ids: list[int]
    project_ids: list[int]
    use_case_id: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        def list_type(vals: Sequence[Any], type_classes: Sequence[Any]) -> bool:
            return isinstance(vals, list) and all(
                isinstance(v, tuple(type_classes)) for v in vals
            )

        if not list_type(self.org_ids, (int,)):
            raise ParsingException("MQL context: org_ids must be a list of integers")

        if not list_type(self.project_ids, (int,)):
            raise ParsingException(
                "MQL context: project_ids must be a list of integers"
            )

        if self.use_case_id is not None and not isinstance(self.use_case_id, str):
            raise ParsingException("MQL context: use_case_id must be a str")
