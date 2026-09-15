import base64
import binascii
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


def extract_message_meta(in_msg: ProtobufMessage) -> RequestMeta:
    if isinstance(in_msg, CreateSubscriptionRequest):
        return in_msg.time_series_request.meta
    if hasattr(in_msg, "meta") and in_msg.HasField("meta") and isinstance(in_msg.meta, RequestMeta):
        return in_msg.meta
    raise ValueError(f"Invalid message type: {type(in_msg)}")


def encode_routing_hint(tier: Tier) -> str:
    """
    Encode the tier a TraceItemTable query read from into the opaque routing_hint
    returned to the client, who passes it to TraceItemDetails so the lookup reads
    the same tier instead of 404ing on rows past tier 1's retention.

    Format: urlsafe base64 of {"v": version, "tier": int, "ts": unix seconds}.
    `ts` only varies the string so clients treat it as opaque; decode ignores it.
    A tier (not a storage name) is encoded so hints survive storage cutovers.

    Every page of a paginated query gets its own hint since pages can route
    differently. Flextime routing never changes the tier, so its hints always
    carry the default tier.
    """
    # TIER_NO_TIER is served by the unsampled table, same as TIER_1
    if tier == Tier.TIER_NO_TIER:
        tier = Tier.TIER_1
    payload = {"v": _ROUTING_HINT_VERSION, "tier": tier.value, "ts": int(time.time())}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_routing_hint(hint: str) -> Tier:
    # Note: unsigned, a caller can forge a hint for any tier; add an HMAC if
    # hints ever gate more than single-item lookups
    try:
        payload = json.loads(base64.urlsafe_b64decode(hint.encode()))
        if payload["v"] != _ROUTING_HINT_VERSION:
            raise ValueError("unsupported version")
        tier = Tier(payload["tier"])
        if tier == Tier.TIER_NO_TIER:
            raise ValueError("invalid tier")
        return tier
    except (binascii.Error, UnicodeError, ValueError, TypeError, KeyError) as e:
        raise BadSnubaRPCRequestException(f"invalid routing_hint: {hint!r}") from e
