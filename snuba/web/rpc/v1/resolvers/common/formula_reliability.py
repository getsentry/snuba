from typing import Any, Dict

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
    ) -> None:
        """
        Inputs:
            expressions: list of expressions requested by the user in the protobuf TimeSeriesRequest.expressions field
            clickhouse_data: the data returned from the clickhouse query
        """
        vis = GetSubformulaLabelsVisitor()
        vis.visit(request)
        children = vis.labels

        self.reliabilities: dict[str, list[Reliability.ValueType]] = {}
        for row in clickhouse_data:
            curr_row_reliabilities: dict[str, Reliability.ValueType] = {}
            for formula, children_labels in children.items():
                for child in children_labels:
                    context = ExtrapolationContext.from_row(child, row)
                    if formula not in curr_row_reliabilities:
                        curr_row_reliabilities[formula] = context.reliability
                    else:
                        curr_row_reliabilities[formula] = min(
                            context.reliability,
                            curr_row_reliabilities[formula],
                            key=reliability_priority,
                        )
                if formula not in self.reliabilities:
                    self.reliabilities[formula] = []
                self.reliabilities[formula].append(curr_row_reliabilities[formula])
        self.keys_cache = set(self.reliabilities.keys())

    def get(self, label: str) -> list[Reliability.ValueType]:
        if label not in self.reliabilities:
            raise ValueError(f"Label {label} not found in reliabilities")
        return self.reliabilities[label]

    def __contains__(self, value: str) -> bool:
        return value in self.keys_cache


def reliability_priority(reliablity: Reliability.ValueType) -> int:
    priority_map = {
        Reliability.RELIABILITY_HIGH: 2,
        Reliability.RELIABILITY_LOW: 1,
        Reliability.RELIABILITY_UNSPECIFIED: 0,
    }
    return priority_map[reliablity]
