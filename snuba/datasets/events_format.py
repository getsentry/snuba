from datetime import datetime, timedelta
from typing import Any, Mapping, MutableMapping, Sequence, Tuple

from snuba import settings
from snuba.processor import (
    _as_dict_safe,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)


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


def extract_http(output: MutableMapping[str, Any], request: Mapping[str, Any]) -> None:
    http_headers = _as_dict_safe(request.get("headers", None))
    output["http_method"] = _unicodify(request.get("method", None))
    output["http_referer"] = _unicodify(http_headers.get("Referer", None))
    output["http_url"] = _unicodify(request.get("url", None))


def extract_extra_tags(tags) -> Tuple[Sequence[str], Sequence[str]]:
    tag_keys = []
    tag_values = []
    for tag_key, tag_value in sorted(tags.items()):
        value = _unicodify(tag_value)
        if value:
            tag_keys.append(_unicodify(tag_key))
            tag_values.append(value)

    return (tag_keys, tag_values)


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
