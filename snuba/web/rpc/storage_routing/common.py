import base64
import json
import time

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba.downsampled_storage_tiers import Tier
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_ROUTING_HINT_VERSION = 1
_ROUTING_HINT_TIERS = frozenset(tier.value for tier in Tier if tier != Tier.TIER_NO_TIER)
_ROUTING_HINT_DEFAULT_TIER = Tier.TIER_1
_INVALID_ROUTING_HINT = "invalid routing_hint"


def extract_message_meta(in_msg: ProtobufMessage) -> RequestMeta:
    if isinstance(in_msg, CreateSubscriptionRequest):
        return in_msg.time_series_request.meta
    if hasattr(in_msg, "meta") and in_msg.HasField("meta") and isinstance(in_msg.meta, RequestMeta):
        return in_msg.meta
    raise ValueError(f"Invalid message type: {type(in_msg)}")


def encode_routing_hint(tier: Tier) -> str:
    # TIER_NO_TIER is served by the unsampled table, same as TIER_1
    if tier not in _ROUTING_HINT_TIERS:
        tier = _ROUTING_HINT_DEFAULT_TIER
    payload = json.dumps(
        {"v": _ROUTING_HINT_VERSION, "tier": tier.value, "ts": int(time.time())}
    ).encode()
    return base64.b64encode(payload).decode()


def decode_routing_hint(hint: str) -> Tier:
    if not hint:
        return _ROUTING_HINT_DEFAULT_TIER

    try:
        decoded = base64.b64decode(hint, validate=True)
        payload = json.loads(decoded)
    except ValueError as e:
        raise BadSnubaRPCRequestException(_INVALID_ROUTING_HINT) from e

    if not isinstance(payload, dict) or payload.get("v") != _ROUTING_HINT_VERSION:
        raise BadSnubaRPCRequestException(_INVALID_ROUTING_HINT)
    tier_value = payload.get("tier")
    if type(tier_value) is not int or tier_value not in _ROUTING_HINT_TIERS:
        raise BadSnubaRPCRequestException(_INVALID_ROUTING_HINT)

    return Tier(tier_value)
