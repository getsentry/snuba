from datetime import datetime
from typing import Any, Dict

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import Reliability

from snuba.web.rpc.v1.resolvers.common.aggregation import ExtrapolationContext
from snuba.web.rpc.v1.visitors.time_series_request_visitor import (
    GetSubformulaLabelsVisitor,
)


class FormulaReliabilityCalculator:
    def __init__(
        self,
        request: TimeSeriesRequest,
        clickhouse_data: list[Dict[str, Any]],
        time_buckets: list[Timestamp],
    ) -> None:
        """
        Inputs:
            expressions: list of expressions requested by the user in the protobuf TimeSeriesRequest.expressions field
            result_timeseries: the data returned from the clickhouse query
            time_buckets: a sorted list of time buckets for the query
        """
        vis = GetSubformulaLabelsVisitor()
        vis.visit(request)
        children = vis.labels
        # stores the keys of formulas that will have the reliability calculated
        self.keys_cache = set(children.keys())
        if len(self.keys_cache) == 0:
            # theres no formulas to calculate reliability for
            return
        # a list of reliability contexts for each time bucket
        self.contexts: list[FormulaReliabilityContext | None] = []

        context_map: dict[int, FormulaReliabilityContext | None] = {}
        for row in clickhouse_data:
            reliability_context = FormulaReliabilityContext.from_row(children, row)
            context_map[reliability_context.bucket] = reliability_context

        for bucket in time_buckets:
            if bucket.seconds in context_map:
                self.contexts.append(context_map[bucket.seconds])
            else:
                self.contexts.append(None)

    def get(self, label: str) -> list[Reliability.ValueType]:
        """
        Returns:
            a list of reliabilities for the formula for each time bucket
        """
        res = []
        for bucket_context in self.contexts:
            if bucket_context is None:
                res.append(Reliability.RELIABILITY_UNSPECIFIED)
            else:
                res.append(bucket_context.get(label))
        return res

    def __contains__(self, value: str) -> bool:
        return value in self.keys_cache


def reliability_priority(reliablity: Reliability.ValueType) -> int:
    priority_map = {
        Reliability.RELIABILITY_HIGH: 2,
        Reliability.RELIABILITY_LOW: 1,
        Reliability.RELIABILITY_UNSPECIFIED: 0,
    }
    return priority_map[reliablity]


class FormulaReliabilityContext:
    # the reliabilities of the formulas for this current row of data
    formula_reliabilities: dict[str, Reliability.ValueType]
    bucket: int

    def __init__(self) -> None:
        self.formula_reliabilities = {}
        self.bucket = -1

    def get(self, label: str) -> Reliability.ValueType:
        return self.formula_reliabilities[label]

    @staticmethod
    def from_row(
        formulas_to_children: dict[str, list[str]], row: dict[str, Any]
    ) -> "FormulaReliabilityContext":
        """
        Inputs:
            formulas_to_children: a dictionary that maps the formula keys their childrens keys
                ex: {"sum(k) + sum(f)": ["sum(k)", "sum(f)"]}

            row: a row of data from the clickhouse query

        Returns:
            a FormulaReliabilityContext object
        """
        reliability_context = FormulaReliabilityContext()

        reliability_context.bucket = int(
            datetime.fromisoformat(row["time"]).timestamp()
        )

        for formula, children in formulas_to_children.items():
            for child in children:
                context = ExtrapolationContext.from_row(child, row)
                if formula not in reliability_context.formula_reliabilities:
                    reliability_context.formula_reliabilities[
                        formula
                    ] = context.reliability
                else:
                    reliability_context.formula_reliabilities[formula] = min(
                        context.reliability,
                        reliability_context.formula_reliabilities[formula],
                        key=reliability_priority,
                    )
        return reliability_context
