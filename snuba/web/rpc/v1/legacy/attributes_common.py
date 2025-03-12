from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import settings, state


def should_use_items_attrs(request_meta: RequestMeta) -> bool:
    if getattr(settings, "USE_EAP_ITEMS_TABLE", False):
        return True
    if request_meta.referrer.startswith("force_use_eap_spans_table"):
        return False

    use_eap_items_attrs_table_start_timestamp_seconds = state.get_int_config(
        "use_eap_items_attrs_table_start_timestamp_seconds"
    )
    if (
        state.get_config("use_eap_items_attrs_table", False)
        and use_eap_items_attrs_table_start_timestamp_seconds is not None
    ):
        return (
            request_meta.start_timestamp.seconds
            >= use_eap_items_attrs_table_start_timestamp_seconds
        )

    return False
