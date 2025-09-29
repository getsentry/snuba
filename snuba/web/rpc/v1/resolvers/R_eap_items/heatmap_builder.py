import uuid
from collections import defaultdict

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    Heatmap,
    HeatmapRequest,
    MatrixColumn,
    TraceItemStatsRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ExistsFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, count, literal, minus
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import setup_trace_query_settings
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
    attribute_key_to_expression,
)

EAP_ITEMS_ENTITY = Entity(
    key=EntityKey("eap_items"),
    schema=get_entity(EntityKey("eap_items")).get_data_model(),
    sample=None,
)

COUNT_LABEL = "count()"


class HeatmapBuilder:
    def __init__(
        self,
        heatmap: HeatmapRequest,
        in_msg: TraceItemStatsRequest,
        routing_decision: RoutingDecision,
        timer: Timer,
        max_buckets: int,
    ):
        self.heatmap = heatmap
        self.in_msg = in_msg
        self.routing_decision = routing_decision
        self.timer = timer
        self.MAX_BUCKETS = max_buckets

    def _build_snuba_request(self, query: Query) -> SnubaRequest:
        query_settings = (
            setup_trace_query_settings() if self.in_msg.meta.debug else HTTPQuerySettings()
        )
        self.routing_decision.strategy.merge_clickhouse_settings(
            self.routing_decision, query_settings
        )
        query_settings.set_sampling_tier(self.routing_decision.tier)

        return SnubaRequest(
            id=uuid.UUID(self.in_msg.meta.request_id),
            original_body=MessageToDict(self.in_msg),
            query=query,
            query_settings=query_settings,
            attribution_info=AttributionInfo(
                referrer=self.in_msg.meta.referrer,
                team="eap",
                feature="eap",
                tenant_ids={
                    "organization_id": self.in_msg.meta.organization_id,
                    "referrer": self.in_msg.meta.referrer,
                },
                app_id=AppID("eap"),
                parent_api="eap_attribute_stats",
            ),
        )

    def _get_min_max_bucketsize_y(
        self,
        heatmap: HeatmapRequest,
        in_msg: TraceItemStatsRequest,
        routing_decision: RoutingDecision,
        timer: Timer,
    ) -> tuple[float | None, float | None, float | None]:
        """
        Returns the min value, max value, and bucket size for the y attribute.

        example:
        say we are building a heatmap for x attribute span.op and y attribute span.duration
        we will get the min span.duration across all spans (items) and the max, then we will use
        number_y_buckets to calculate the bucket size.
        say min is 25, max is 350 and number_y_buckets is 4
        then we will return (25, 350, 81.25)
        """
        x_attribute = heatmap.x_attribute
        y_attribute = heatmap.y_attribute
        y_attribute_val = attribute_key_to_expression(y_attribute)
        filter = TraceItemFilter(
            and_filter=AndFilter(
                filters=[
                    in_msg.filter,
                    TraceItemFilter(exists_filter=ExistsFilter(key=x_attribute)),
                    TraceItemFilter(exists_filter=ExistsFilter(key=y_attribute)),
                ]
            )
        )
        filter_expression = trace_item_filters_to_expression(
            filter,
            (attribute_key_to_expression),
        )
        condition = base_conditions_and(in_msg.meta, filter_expression)
        min_max_query = Query(
            from_clause=EAP_ITEMS_ENTITY,
            selected_columns=[
                SelectedExpression(name="max_val_y", expression=f.max(y_attribute_val)),
                SelectedExpression(name="min_val_y", expression=f.min(y_attribute_val)),
            ],
            condition=condition,
        )
        treeify_or_and_conditions(min_max_query)
        snuba_request = self._build_snuba_request(min_max_query)
        min_max_res = (
            run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=timer,
            )
            .result["data"][0]
            .values()
        )
        if list(min_max_res) == [None, None]:
            return (None, None, None)
        min_val_y = min(min_max_res)
        max_val_y = max(min_max_res)
        group_size_y = (max_val_y - min_val_y) / heatmap.num_y_buckets
        return (min_val_y, max_val_y, group_size_y)

    def _get_y_buckets(
        self, min_y: float, max_y: float, group_size_y: float
    ) -> list[AttributeValue]:
        buckets = [
            AttributeValue(
                val_str=f"[{min_y + i * group_size_y}, {min_y + (i + 1) * group_size_y})",
            )
            for i in range(self.heatmap.num_y_buckets)
        ]
        buckets[-1].val_str = buckets[-1].val_str[:-1] + "]"
        return buckets

    def _build_heatmap_query(
        self,
        min_val_y: float,
        max_val_y: float,
        group_size_y: float,
    ) -> Query:
        x_attribute = self.heatmap.x_attribute
        y_attribute = self.heatmap.y_attribute
        x_attribute_val = attribute_key_to_expression(x_attribute)
        y_attribute_val = attribute_key_to_expression(y_attribute)
        filter = TraceItemFilter(
            and_filter=AndFilter(
                filters=[
                    self.in_msg.filter,
                    TraceItemFilter(exists_filter=ExistsFilter(key=x_attribute)),
                    TraceItemFilter(exists_filter=ExistsFilter(key=y_attribute)),
                ]
            )
        )
        filter_expression = trace_item_filters_to_expression(
            filter,
            (attribute_key_to_expression),
        )
        condition = base_conditions_and(self.in_msg.meta, filter_expression)
        bucket_index_y = f.least(
            f.intDiv(minus(y_attribute_val, literal(min_val_y)), literal(group_size_y)),
            literal(self.heatmap.num_y_buckets - 1),
            alias="bucket_index_y",
        )
        selected_columns = [
            SelectedExpression(name="bucket_index_y", expression=bucket_index_y),
            SelectedExpression(name="x_attribute_val", expression=x_attribute_val),
            # SelectedExpression(name="y_attribute_val", expression=y_attribute_val),
            SelectedExpression(
                name=COUNT_LABEL,
                expression=count(alias="_count"),
            ),
        ]

        query = Query(
            from_clause=EAP_ITEMS_ENTITY,
            selected_columns=selected_columns,
            condition=condition,
            order_by=[
                OrderBy(
                    direction=OrderByDirection.ASC,
                    expression=x_attribute_val,
                ),
                OrderBy(
                    direction=OrderByDirection.ASC,
                    expression=column("bucket_index_y"),
                ),
            ],
            groupby=[
                x_attribute_val,
                column("bucket_index_y"),
                # column("bucket_start_y"),
                # column("bucket_end_y"),
            ],
        )
        # this function call is needed for legacy reasons
        treeify_or_and_conditions(query)
        return query

    def _transform_results(
        self, query_res: QueryResult, min_y: float, max_y: float, group_size_y: float
    ) -> Heatmap:
        heatmap_data: defaultdict[str, list[float]] = defaultdict(
            lambda: [0.0] * self.heatmap.num_y_buckets
        )
        x_labels_ordered: list[str] = []
        for row in query_res.result["data"]:
            if len(x_labels_ordered) == 0 or x_labels_ordered[-1] != row["x_attribute_val"]:
                x_labels_ordered.append(row["x_attribute_val"])
            heatmap_data[row["x_attribute_val"]][row["bucket_index_y"]] = row["count()"]

        data: list[MatrixColumn] = []
        for x_label in x_labels_ordered:
            data.append(
                MatrixColumn(x_label=AttributeValue(val_str=x_label), values=heatmap_data[x_label])
            )
        heatmap = Heatmap(
            x_attribute=self.heatmap.x_attribute,
            y_attribute=self.heatmap.y_attribute,
            y_buckets=self._get_y_buckets(min_y, max_y, group_size_y),
            data=data,
        )
        return heatmap

    def build(self) -> Heatmap:
        heatmap = self.heatmap
        if (
            heatmap.x_attribute.type == AttributeKey.TYPE_INT
            and heatmap.num_x_buckets > self.MAX_BUCKETS
        ) or (
            heatmap.y_attribute.type == AttributeKey.TYPE_INT
            and heatmap.num_y_buckets > self.MAX_BUCKETS
        ):
            raise BadSnubaRPCRequestException(f"Max allowed buckets is {self.MAX_BUCKETS}.")

        min_y, max_y, group_size_y = self._get_min_max_bucketsize_y(
            heatmap, self.in_msg, self.routing_decision, self.timer
        )
        if min_y is None or max_y is None or group_size_y is None:
            return Heatmap(
                x_attribute=heatmap.x_attribute,
                y_attribute=heatmap.y_attribute,
                y_buckets=[],
                data=[],
            )
        query = self._build_heatmap_query(min_y, max_y, group_size_y)

        # run the query
        snuba_request = self._build_snuba_request(query)
        query_res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self.timer,
        )
        self.routing_decision.routing_context.query_result = query_res
        res_heatmap = self._transform_results(query_res, min_y, max_y, group_size_y)
        return res_heatmap
