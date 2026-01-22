from __future__ import annotations

import base64
import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import (
    Any,
    Generic,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from uuid import UUID

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import ExtrapolationMode

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity as EntityDS
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import SubscriptionQuerySettings
from snuba.reader import Result
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.proto_visitor import (
    GetExpressionAggregationsVisitor,
    TimeSeriesExpressionWrapper,
)
from snuba.web.rpc.v1.endpoint_time_series import (
    EndpointTimeSeries,
    _convert_aggregations_to_expressions,
)

SUBSCRIPTION_REFERRER = "subscription"

# These are subscription payload keys which need to be set as attributes in SubscriptionData.
SUBSCRIPTION_DATA_PAYLOAD_KEYS = {
    "project_id",
    "time_window",
    "resolution",
    "query",
    "tenant_ids",
    "subscription_type",
    "time_series_request",
    "request_name",
    "request_version",
}

REQUEST_TYPE_ALLOWLIST = [("TimeSeriesRequest", "v1")]


class SubscriptionType(Enum):
    SNQL = "snql"
    RPC = "rpc"


logger = logging.getLogger("snuba.subscriptions")

PartitionId = NewType("PartitionId", int)

TRequest = TypeVar("TRequest")


@dataclass(frozen=True)
class SubscriptionIdentifier:
    partition: PartitionId
    uuid: UUID

    def __str__(self) -> str:
        return f"{self.partition}/{self.uuid.hex}"

    @classmethod
    def from_string(cls, value: str) -> SubscriptionIdentifier:
        partition, uuid = value.split("/")
        return cls(PartitionId(int(partition)), UUID(uuid))


@dataclass(frozen=True, kw_only=True)
class _SubscriptionData(ABC, Generic[TRequest]):
    project_id: int
    resolution_sec: int
    time_window_sec: int
    entity: Entity
    metadata: Mapping[str, Any]
    tenant_ids: Mapping[str, Any] = field(default_factory=lambda: dict())

    def validate(self) -> None:
        if self.time_window_sec < 60:
            raise InvalidSubscriptionError("Time window must be greater than or equal to 1 minute")
        elif self.time_window_sec > 60 * 60 * 24:
            raise InvalidSubscriptionError("Time window must be less than or equal to 24 hours")

        if self.resolution_sec < 60:
            raise InvalidSubscriptionError("Resolution must be greater than or equal to 1 minute")

    @abstractmethod
    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> TRequest:
        raise NotImplementedError

    @abstractmethod
    def run_query(
        self,
        dataset: Dataset,
        request: TRequest,
        timer: Timer,
        robust: bool = False,
        concurrent_queries_gauge: Optional[Gauge] = None,
    ) -> QueryResult:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_dict(
        cls, data: Mapping[str, Any], entity_key: EntityKey
    ) -> _SubscriptionData[TRequest]:
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, kw_only=True)
class RPCSubscriptionData(_SubscriptionData[TimeSeriesRequest]):
    """
    Represents the state of an RPC subscription.
    """

    time_series_request: str

    request_name: str
    request_version: str

    def __post_init__(self) -> None:
        # convert any use of aggregation to expressions
        request = TimeSeriesRequest()
        request.ParseFromString(base64.b64decode(self.time_series_request))
        request = _convert_aggregations_to_expressions(request)
        object.__setattr__(
            self,
            "time_series_request",
            base64.b64encode(request.SerializeToString()).decode("utf-8"),
        )

    def validate(self) -> None:
        super().validate()
        if (self.request_name, self.request_version) not in REQUEST_TYPE_ALLOWLIST:
            raise InvalidSubscriptionError(
                f"{self.request_name} {self.request_version} not supported."
            )

        request = TimeSeriesRequest()
        request.ParseFromString(base64.b64decode(self.time_series_request))

        if not (request.meta) or len(request.meta.project_ids) == 0:
            raise InvalidSubscriptionError("Project ID is required.")

        if len(request.meta.project_ids) != 1:
            raise InvalidSubscriptionError("Multiple project IDs not supported.")

        if not request.expressions or len(request.expressions) != 1:
            raise InvalidSubscriptionError("Exactly one expression required.")

        if request.group_by:
            raise InvalidSubscriptionError("Group bys not supported.")

        expression = request.expressions[0]
        vis = GetExpressionAggregationsVisitor()
        TimeSeriesExpressionWrapper(expression).accept(vis)
        allowed_modes = [
            ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ExtrapolationMode.EXTRAPOLATION_MODE_CLIENT_ONLY,
            ExtrapolationMode.EXTRAPOLATION_MODE_SERVER_ONLY,
        ]
        if any(e.extrapolation_mode not in allowed_modes for e in vis.aggregations):
            raise InvalidSubscriptionError(
                f"Invalid extrapolation mode. Allowed extrapolation modes: {', '.join([ExtrapolationMode.Name(mode) for mode in allowed_modes])}"
            )

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> TimeSeriesRequest:
        request_class = EndpointTimeSeries().request_class()()
        request_class.ParseFromString(base64.b64decode(self.time_series_request))

        start_time_proto = Timestamp()
        start_time_proto.FromDatetime(timestamp - timedelta(seconds=self.time_window_sec))
        end_time_proto = Timestamp()
        end_time_proto.FromDatetime(timestamp)
        request_class.meta.start_timestamp.CopyFrom(start_time_proto)
        request_class.meta.end_timestamp.CopyFrom(end_time_proto)

        request_class.meta.referrer = referrer

        request_class.granularity_secs = self.time_window_sec

        return request_class

    def run_query(
        self,
        dataset: Dataset,
        request: TimeSeriesRequest,
        timer: Timer,
        robust: bool = False,
        concurrent_queries_gauge: Optional[Gauge] = None,
    ) -> QueryResult:
        response = EndpointTimeSeries().execute(request)
        if not response.result_timeseries or not any(
            dp.data_present for dp in response.result_timeseries[0].data_points
        ):
            result: Result = {
                "meta": [],
                "data": [{request.expressions[0].label: None}],
                "trace_output": "",
            }
            return QueryResult(result=result, extra={"stats": {}, "sql": "", "experiments": {}})

        timeseries = response.result_timeseries[0]
        data = [{timeseries.label: timeseries.data_points[0].data}]

        result = {"meta": [], "data": data, "trace_output": ""}
        return QueryResult(result=result, extra={"stats": {}, "sql": "", "experiments": {}})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], entity_key: EntityKey) -> RPCSubscriptionData:
        entity: Entity = get_entity(entity_key)
        metadata = {}
        for key in data.keys():
            if key == "metadata":
                metadata.update(data[key])
            elif key not in SUBSCRIPTION_DATA_PAYLOAD_KEYS:
                metadata[key] = data[key]

        return RPCSubscriptionData(
            project_id=data["project_id"],
            time_window_sec=int(data["time_window"]),
            resolution_sec=int(data["resolution"]),
            time_series_request=data["time_series_request"],
            request_version=data["request_version"],
            request_name=data["request_name"],
            entity=entity,
            metadata=metadata,
            tenant_ids=data.get("tenant_ids", dict()),
        )

    @classmethod
    def from_proto(
        cls, item: CreateSubscriptionRequest, entity_key: EntityKey
    ) -> RPCSubscriptionData:
        entity: Entity = get_entity(entity_key)
        request_class = item.time_series_request.__class__
        class_name = request_class.__name__
        class_version = request_class.__module__.split(".", 3)[2]

        metadata = {}
        if item.time_series_request.meta:
            metadata["organization"] = item.time_series_request.meta.organization_id

        return RPCSubscriptionData(
            project_id=item.time_series_request.meta.project_ids[0],
            time_window_sec=item.time_window_secs,
            resolution_sec=item.resolution_secs,
            time_series_request=base64.b64encode(
                item.time_series_request.SerializeToString()
            ).decode("utf-8"),
            entity=entity,
            metadata=metadata,
            tenant_ids={},
            request_version=class_version,
            request_name=class_name,
        )

    def to_dict(self) -> Mapping[str, Any]:
        subscription_data_dict = {
            "project_id": self.project_id,
            "time_window": self.time_window_sec,
            "resolution": self.resolution_sec,
            "time_series_request": self.time_series_request,
            "request_version": self.request_version,
            "request_name": self.request_name,
            "subscription_type": SubscriptionType.RPC.value,
        }
        if self.metadata:
            subscription_data_dict["metadata"] = self.metadata

        return subscription_data_dict


@dataclass(frozen=True, kw_only=True)
class SnQLSubscriptionData(_SubscriptionData[Request]):
    """
    Represents the state of a subscription.
    """

    query: str

    def add_conditions(
        self,
        timestamp: datetime,
        offset: Optional[int],
        query: Union[CompositeQuery[EntityDS], Query],
    ) -> None:
        added_timestamp_column = False
        from_clause = query.get_from_clause()
        entities: List[Tuple[Optional[str], Entity]] = []
        if isinstance(from_clause, JoinClause):
            for alias, node in from_clause.get_alias_node_map().items():
                assert isinstance(node.data_source, EntityDS), node.data_source
                entities.append((alias, get_entity(node.data_source.key)))
        elif isinstance(from_clause, EntityDS):
            entities = [(None, get_entity(from_clause.key))]
        else:
            raise InvalidSubscriptionError("Only simple queries and join queries are supported")
        for entity_alias, entity in entities:
            conditions_to_add: List[Expression] = [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, entity_alias, "project_id"),
                    Literal(None, self.project_id),
                ),
            ]

            required_timestamp_column = entity.required_time_column
            if required_timestamp_column is not None:
                added_timestamp_column = True
                conditions_to_add.extend(
                    [
                        binary_condition(
                            ConditionFunctions.GTE,
                            Column(None, entity_alias, required_timestamp_column),
                            Literal(
                                None,
                                (timestamp - timedelta(seconds=self.time_window_sec)),
                            ),
                        ),
                        binary_condition(
                            ConditionFunctions.LT,
                            Column(None, entity_alias, required_timestamp_column),
                            Literal(None, timestamp),
                        ),
                    ]
                )

            new_condition = combine_and_conditions(conditions_to_add)
            condition = query.get_condition()
            if condition:
                new_condition = binary_condition(BooleanFunctions.AND, condition, new_condition)

            query.set_ast_condition(new_condition)

            subscription_processors = self.entity.get_subscription_processors()
            if subscription_processors:
                for processor in subscription_processors:
                    processor.process(query, self.metadata, offset)

        if not added_timestamp_column:
            raise InvalidSubscriptionError(
                "At least one Entity must have a timestamp column for subscriptions"
            )

    def build_request(
        self,
        dataset: Dataset,
        timestamp: datetime,
        offset: Optional[int],
        timer: Timer,
        metrics: Optional[MetricsBackend] = None,
        referrer: str = SUBSCRIPTION_REFERRER,
    ) -> Request:
        schema = RequestSchema.build(SubscriptionQuerySettings)

        custom_processing = []
        subscription_validators = self.entity.get_subscription_validators()
        if subscription_validators:
            for validator in subscription_validators:
                custom_processing.append(validator.validate)
        custom_processing.append(partial(self.add_conditions, timestamp, offset))

        tenant_ids = {**self.tenant_ids}
        tenant_ids["referrer"] = referrer
        if "organization_id" not in tenant_ids:
            # TODO: Subscriptions queries should have an org ID
            tenant_ids["organization_id"] = 1

        request = build_request(
            {
                "query": self.query,
                "tenant_ids": tenant_ids,
            },
            parse_snql_query,
            SubscriptionQuerySettings,
            schema,
            dataset,
            timer,
            referrer,
            # subscriptions are tied to entities, these validators are going to run on the entity
            # anyways so it's okay that the post-processing is done without type safety
            custom_processing,  # type: ignore
        )
        return request

    def run_query(
        self,
        dataset: Dataset,
        request: Request,
        timer: Timer,
        robust: bool = False,
        concurrent_queries_gauge: Optional[Gauge] = None,
    ) -> QueryResult:
        return run_query(
            dataset,
            request,
            timer,
            robust=robust,
            concurrent_queries_gauge=concurrent_queries_gauge,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], entity_key: EntityKey) -> SnQLSubscriptionData:
        entity: Entity = get_entity(entity_key)

        metadata = {}
        for key in data.keys():
            if key not in SUBSCRIPTION_DATA_PAYLOAD_KEYS:
                metadata[key] = data[key]

        return SnQLSubscriptionData(
            project_id=data["project_id"],
            # Always cast to int in case any old subscriptions are stored as float
            time_window_sec=int(data["time_window"]),
            resolution_sec=int(data["resolution"]),
            query=data["query"],
            entity=entity,
            metadata=metadata,
            tenant_ids=data.get("tenant_ids", dict()),
        )

    def to_dict(self) -> Mapping[str, Any]:
        subscription_data_dict = {
            "project_id": self.project_id,
            "time_window": self.time_window_sec,
            "resolution": self.resolution_sec,
            "query": self.query,
            "subscription_type": SubscriptionType.SNQL.value,
        }

        subscription_processors = self.entity.get_subscription_processors()
        if subscription_processors:
            for processor in subscription_processors:
                subscription_data_dict.update(processor.to_dict(self.metadata))
        return subscription_data_dict


SubscriptionData = Union[RPCSubscriptionData, SnQLSubscriptionData]
SubscriptionRequest = Union[Request, TimeSeriesRequest]


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData


class SubscriptionWithMetadata(NamedTuple):
    entity: EntityKey
    subscription: Subscription
    tick_upper_offset: int


@dataclass(frozen=True)
class ScheduledSubscriptionTask:
    """
    A scheduled task represents a unit of work (a task) that is intended to
    be executed at (or around) a specific time.
    """

    # The time that this task was scheduled to execute.
    timestamp: datetime

    # The task that should be executed.
    task: SubscriptionWithMetadata


class SubscriptionScheduler(ABC):
    """
    The scheduler maintains the scheduling state for subscription tasks and
    provides the ability to query the schedule to find tasks that should be
    scheduled between the time interval of a tick.
    """

    @abstractmethod
    def find(self, tick: Tick) -> Iterator[ScheduledSubscriptionTask]:
        """
        Find all of the tasks that were scheduled to be executed between the
        lower bound (exclusive) and upper bound (inclusive) of the tick's
        time interval. The tasks returned should be ordered by timestamp in
        ascending order.
        """
        raise NotImplementedError


class SubscriptionTaskResultFuture(NamedTuple):
    task: ScheduledSubscriptionTask
    future: Future[Tuple[SubscriptionRequest, Result]]


class SubscriptionTaskResult(NamedTuple):
    task: ScheduledSubscriptionTask
    result: Tuple[SubscriptionRequest, Result]
