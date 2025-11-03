from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import Reliability

from snuba.web.rpc.v1.resolvers.common.aggregation import ExtrapolationContext
from snuba.web.rpc.v1.visitors.time_series_request_visitor import (
    GetSubformulaLabelsVisitor,
)


@dataclass(frozen=True)
class FormulaExtrapolationContext:
    average_sample_rate: float = 0
    sample_count: int = 0
    reliability: Reliability.ValueType = Reliability.RELIABILITY_UNSPECIFIED


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

    def get(self, label: str) -> list[FormulaExtrapolationContext]:
        """
        Returns:
            a list of reliabilities for the formula for each time bucket
        """
        res: list[FormulaExtrapolationContext] = []
        for bucket_context in self.contexts:
            if bucket_context is None:
                res.append(FormulaExtrapolationContext())
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
    formula_extrapolation_contexts: dict[str, FormulaExtrapolationContext]
    bucket: int

    def __init__(self) -> None:
        self.formula_extrapolation_contexts = {}
        self.bucket = -1

    def get(self, label: str) -> FormulaExtrapolationContext:
        return self.formula_extrapolation_contexts[label]

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

        reliability_context.bucket = int(datetime.fromisoformat(row["time"]).timestamp())

        for formula, children in formulas_to_children.items():
            for child in children:
                context = ExtrapolationContext.from_row(child, row)

                if formula not in reliability_context.formula_extrapolation_contexts:
                    reliability_context.formula_extrapolation_contexts[formula] = (
                        FormulaExtrapolationContext(
                            average_sample_rate=context.average_sample_rate,
                            sample_count=context.sample_count,
                            reliability=context.reliability,
                        )
                    )
                else:
                    reliability_context.formula_extrapolation_contexts[formula] = (
                        FormulaExtrapolationContext(
                            # no way to get the true sample rate so we approximate it
                            # by taking the max sample count of the parts of the formula
                            average_sample_rate=max(
                                context.average_sample_rate,
                                reliability_context.formula_extrapolation_contexts[
                                    formula
                                ].average_sample_rate,
                            ),
                            # no way to get the true sample count so we approximate it
                            # by taking the max sample count of the parts of the formula
                            sample_count=max(
                                context.sample_count,
                                reliability_context.formula_extrapolation_contexts[
                                    formula
                                ].sample_count,
                            ),
                            # taking the lowest reliability of the parts of the formula
                            # gives us the reliability of the overall formula
                            reliability=min(
                                context.reliability,
                                reliability_context.formula_extrapolation_contexts[
                                    formula
                                ].reliability,
                                key=reliability_priority,
                            ),
                        )
                    )

        return reliability_context
