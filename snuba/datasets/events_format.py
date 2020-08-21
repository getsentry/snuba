from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, MutableMapping, Sequence, Tuple, TypeVar

from snuba import settings
from snuba.processor import _ensure_valid_date, _ensure_valid_ip, _unicodify


def extract_project_id(
    output: MutableMapping[str, Any], event: Mapping[str, Any]
) -> None:
    output["project_id"] = event["project_id"]


def extract_base(output, message):
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


def extract_extra_tags(
    nested_col: Mapping[str, Any],
) -> Tuple[Sequence[str], Sequence[str]]:
    return extract_nested(nested_col, lambda s: _unicodify(s))


def extract_nested(
    nested_col: Mapping[str, Any], val_processor: Callable[[Any], TVal]
) -> Tuple[Sequence[str], Sequence[TVal]]:
    keys = []
    values = []
    for key, value in sorted(nested_col.items()):
        value = val_processor(value)
        if value:
            keys.append(_unicodify(key))
            values.append(value)

    return (keys, values)


def extract_extra_contexts(contexts) -> Tuple[Sequence[str], Sequence[str]]:
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
                        context_keys.append(_unicodify(ctx_key))
                        context_values.append(_unicodify(ctx_value))

    return (context_keys, context_values)


def enforce_retention(message, timestamp):
    project_id = message["project_id"]
    retention_days = settings.RETENTION_OVERRIDES.get(project_id)
    if retention_days is None:
        retention_days = int(
            message.get("retention_days") or settings.DEFAULT_RETENTION_DAYS
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


def escape_field(field: str) -> str:
    """
    We have ':' in our tag names. Also we may have '|'. This escapes : and \ so
    that we can always rebuild the tags from the map. When looking for tags with LIKE
    there should be no issue. But there may be other cases.
    """
    return field.translate(ESCAPE_TRANSLATION)


def flatten_nested_field(keys: Sequence[str], values: Sequence[str]) -> str:
    # We need to guarantee the content of the merged string is sorted otherwise we
    # will not be able to run a LIKE operation over multiple fields at the same time.
    # Tags are pre sorted, but it seems contexts are not, so to make this generic
    # we ensure the invariant is respected here.
    pairs = sorted(zip(keys, values))
    str_pairs = [f"|{escape_field(k)}={escape_field(v)}|" for k, v in pairs]
    # The result is going to be:
    # |tag:val||tag:val|
    # This gives the guarantee we will always have a delimiter on both side of the
    # tag pair, thus we can unequivocally identify a tag with a LIKE expression even if
    # the value or the tag name in the query is a substring of a real tag.
    return "".join(str_pairs)


class EventTooOld(Exception):
    pass
