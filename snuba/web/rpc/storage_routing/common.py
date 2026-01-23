from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta


def extract_message_meta(in_msg: ProtobufMessage) -> RequestMeta:
    if isinstance(in_msg, CreateSubscriptionRequest):
        return in_msg.time_series_request.meta
    elif (
        hasattr(in_msg, "meta") and in_msg.HasField("meta") and isinstance(in_msg.meta, RequestMeta)
    ):
        return in_msg.meta
    else:
        raise ValueError(f"Invalid message type: {type(in_msg)}")
