from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from snuba import settings
from snuba.processor import (
    _as_dict_safe,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)
from snuba.utils.serializable_exception import SerializableException


def extract_project_id(
    output: MutableMapping[str, Any], event: Mapping[str, Any]
) -> None:
    output["project_id"] = event["project_id"]


def extract_base(
    output: MutableMapping[str, Any], message: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    output["event_id"] = message["event_id"]
    extract_project_id(output, message)
    return output


def extract_user(output: MutableMapping[str, Any], user: Mapping[str, Any]) -> None:
    output["user_id"] = _unicodify(user.get("id", None))
    output["username"] = _unicodify(user.get("username", None))
    output["email"] = _unicodify(user.get("email", None))
    ip_addr = _ensure_valid_ip(user.get("ip_address", None))
    output["ip_address"] = str(ip_addr) if ip_addr is not None else None


TVal = TypeVar("TVal")


def extract_http(output: MutableMapping[str, Any], request: Mapping[str, Any]) -> None:
    http_headers: Mapping[str, Any] = _as_dict_safe(request.get("headers", None))
    output["http_method"] = _unicodify(request.get("method", None))
    output["http_referer"] = _unicodify(http_headers.get("Referer", None))
    output["http_url"] = _unicodify(request.get("url", None))


def extract_extra_tags(
    nested_col: Mapping[str, Any],
) -> Tuple[Sequence[str], Sequence[str]]:
    return extract_nested(nested_col, lambda s: _unicodify(s) or None)


def extract_nested(
    nested_col: Mapping[str, Any], val_processor: Callable[[Any], Optional[TVal]]
) -> Tuple[Sequence[str], Sequence[TVal]]:
    keys = []
    values = []
    for key, value in sorted(nested_col.items()):
        value = val_processor(value)
        if value is not None:
            unicode_key = _unicodify(key)
            assert isinstance(unicode_key, str)
            keys.append(unicode_key)
            values.append(value)

    return (keys, values)


def extract_extra_contexts(
    contexts: Mapping[str, Any]
) -> Tuple[Sequence[str], Sequence[str]]:
    context_keys = []
    context_values = []
    valid_types = (int, float, str)
    for ctx_name, ctx_obj in contexts.items():
        if isinstance(ctx_obj, dict):
            ctx_obj.pop("type", None)  # ignore type alias
            for inner_ctx_name, ctx_value in ctx_obj.items():
                if isinstance(ctx_value, valid_types):
                    value = _unicodify(ctx_value)
                    if value:
                        ctx_key = f"{ctx_name}.{inner_ctx_name}"
                        unicode_key = _unicodify(ctx_key)
                        unicode_value = _unicodify(ctx_value)

                        assert isinstance(unicode_key, str)
                        assert isinstance(unicode_value, str)

                        context_keys.append(unicode_key)
                        context_values.append(unicode_value)

    return (context_keys, context_values)


def enforce_retention(
    retention_days: Optional[int], timestamp: Optional[datetime]
) -> int:
    if not isinstance(retention_days, int):
        retention_days = settings.DEFAULT_RETENTION_DAYS

    if settings.ENFORCE_RETENTION:
        if retention_days not in settings.VALID_RETENTION_DAYS:
            retention_days = (
                settings.LOWER_RETENTION_DAYS
                if retention_days <= settings.LOWER_RETENTION_DAYS
                else settings.DEFAULT_RETENTION_DAYS
            )

    # This is not ideal but it should never happen anyways
    timestamp = _ensure_valid_date(timestamp)
    if timestamp is None:
        timestamp = datetime.utcnow()
    # TODO: We may need to allow for older events in the future when post
    # processing triggers are based off of Snuba. Or this branch could be put
    # behind a "backfill-only" optional switch.
    if settings.DISCARD_OLD_EVENTS and timestamp < (
        datetime.utcnow() - timedelta(days=retention_days)
    ):
        raise EventTooOld
    return retention_days


ESCAPE_TRANSLATION = str.maketrans({"\\": "\\\\", "|": "\|", "=": "\="})


class EventTooOld(SerializableException):
    pass
