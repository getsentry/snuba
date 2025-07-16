from typing import Any

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest,
)


def extract_message_meta(in_msg: ProtobufMessage) -> Any:
    return (
        in_msg.time_series_request.meta
        if isinstance(in_msg, CreateSubscriptionRequest)
        else in_msg.meta  # type: ignore
    )
