"""
This file contains replicated classes from the snuba-sdk.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from snuba.query import OrderByDirection
from snuba.query.mql.exceptions import InvalidExpressionError, InvalidMQLContextError


class MQLContextValidator:
    def visit(self, query: MQLContext) -> Mapping[str, Any]:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    def _combine(
        self, query: MQLContext, returns: Mapping[str, None | Mapping[str, None]]
    ) -> Mapping[str, Any]:
        return {}

    def _visit_entity(self, entity: str | None) -> None:
        if entity is None:
            raise InvalidMQLContextError("entity is required for a MQL context")
        elif not isinstance(entity, str):
            raise InvalidMQLContextError("entity must be a str")

    def _visit_start(self, start: datetime | None) -> None:
        if start is None:
            raise InvalidMQLContextError("start is required for a MQL context")
        elif not isinstance(start, datetime):
            raise InvalidMQLContextError("start must be a datetime")

    def _visit_end(self, end: datetime | None) -> None:
        if end is None:
            raise InvalidMQLContextError("end is required for a MQL context")
        elif not isinstance(end, datetime):
            raise InvalidMQLContextError("end must be a datetime")

    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, None]:
        if rollup is None:
            raise InvalidMQLContextError("rollup is required for a MQL context")
        elif not isinstance(rollup, Rollup):
            raise InvalidMQLContextError("rollup must be a Rollup object")

        # Since the granularity is inferred by the API, it can be initially None, but must be present when
        # the query is ultimately serialized and sent to Snuba.
        if rollup.granularity is None:
            raise InvalidMQLContextError("granularity must be set on the rollup")

        rollup.validate()
        return {}

    def _visit_scope(self, scope: MetricsScope | None) -> None:
        if scope is None:
            raise InvalidMQLContextError("scope is required for a MQL context")
        elif not isinstance(scope, MetricsScope):
            raise InvalidMQLContextError("scope must be a MetricsScope object")
        scope.validate()

    def _visit_limit(self, limit: Limit | None) -> None:
        if limit is None:
            return
        elif not isinstance(limit, Limit):
            raise InvalidMQLContextError("limit must be a Limit object")

        limit.validate()

    def _visit_offset(self, offset: Offset | None) -> None:
        if offset is None:
            return
        elif not isinstance(offset, Offset):
            raise InvalidMQLContextError("offset must be a Offset object")

        offset.validate()

    def _visit_indexer_mappings(
        self, indexer_mapping: Mapping[str, Any] | None
    ) -> Mapping[str, Any]:
        if indexer_mapping is None:
            return {}
        if not isinstance(indexer_mapping, dict):
            raise InvalidMQLContextError("indexer_mapping must be a dictionary")
        return {}


@dataclass
class MQLContext:
    """
    The MQL string alone is not enough to fully describe a query.
    This class contains all of the additional information needed to
    execute a metrics query in snuba.
    """

    entity: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None
    limit: Limit | None = None
    offset: Offset | None = None
    indexer_mappings: dict[str, Any] | None = None

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)
        return tuple(f.name for f in self_fields)

    def validate(self) -> None:
        VALIDATOR.visit(self)


VALIDATOR = MQLContextValidator()


def _validate_int_literal(
    name: str, literal: int, minn: Optional[int], maxn: Optional[int]
) -> None:
    if not isinstance(literal, int):
        raise InvalidExpressionError(f"{name} '{literal}' must be an integer")
    if minn is not None and literal < minn:
        raise InvalidExpressionError(f"{name} '{literal}' must be at least {minn:,}")
    elif maxn is not None and literal > maxn:
        raise InvalidExpressionError(f"{name} '{literal}' is capped at {maxn:,}")


@dataclass(frozen=True)
class Rollup:
    """
    Rollup instructs how the timeseries queries should be grouped on time. If the query is for a set of timeseries, then
    the interval field should be specified. It is the number of seconds to group the timeseries by.
    For a query that returns only the totals, specify Totals(True). A totals query can be ordered using the orderby field.
    If totals is set to True and the interval is specified, then an extra row will be returned in the result with the totals
    for the timeseries.
    """

    interval: int | None = None
    totals: bool | None = None
    orderby: OrderByDirection | None = (
        None  # TODO: This doesn't make sense: ordered by what?
    )
    granularity: int | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        # The interval is used to determine how the timestamp is rolled up in the group by of the query.
        # The granularity is separate since it ultimately determines which data we retrieve.
        if self.granularity and self.granularity not in ALLOWED_GRANULARITIES:
            raise InvalidExpressionError(
                f"granularity must be an integer and one of {ALLOWED_GRANULARITIES}"
            )

        if self.interval is not None:
            _validate_int_literal(
                "interval", self.interval, 10, None
            )  # Minimum 10 seconds
            if self.granularity is not None and self.interval < self.granularity:
                raise InvalidExpressionError(
                    "interval must be greater than or equal to granularity"
                )

        if self.totals is not None:
            if not isinstance(self.totals, bool):
                raise InvalidExpressionError("totals must be a boolean")

        if self.interval is None and self.totals is None:
            raise InvalidExpressionError(
                "Rollup must have at least one of interval or totals"
            )

        if self.orderby is not None:
            if not isinstance(self.orderby, OrderByDirection):
                raise InvalidExpressionError("orderby must be a Direction object")

        if self.interval is not None and self.orderby is not None:
            raise InvalidExpressionError(
                "Timeseries queries can't be ordered when using interval"
            )


ALLOWED_GRANULARITIES = (10, 60, 3600, 86400)


@dataclass
class MetricsScope:
    """
    This contains all the meta information necessary to resolve a metric and to safely query
    the metrics dataset. All these values get automatically added to the query conditions.
    The idea of this class is to contain all the filter values that are not represented by
    tags in the API.

    use_case_id is treated separately since it can be derived separate from the MRIs of the
    metrics in the outer query.
    """

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
            raise InvalidExpressionError("org_ids must be a list of integers")

        if not list_type(self.project_ids, (int,)):
            raise InvalidExpressionError("project_ids must be a list of integers")

        if self.use_case_id is not None and not isinstance(self.use_case_id, str):
            raise InvalidExpressionError("use_case_id must be an str")


@dataclass(frozen=True)
class Limit:
    limit: int

    def validate(self) -> None:
        _validate_int_literal("limit", self.limit, 1, 10000)


@dataclass(frozen=True)
class Offset:
    offset: int

    def validate(self) -> None:
        _validate_int_literal("offset", self.offset, 0, None)
