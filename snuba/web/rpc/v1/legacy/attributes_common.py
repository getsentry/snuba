from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import settings, state


def should_use_items_attrs(request_meta: RequestMeta) -> bool:
    if getattr(settings, "USE_EAP_ITEMS_TABLE", False):
        return True
    if request_meta.referrer.startswith("force_use_eap_spans_table"):
        return False

    use_eap_items_attrs_table_start_timestamp_seconds = state.get_int_config(
        "use_eap_items_attrs_table_start_timestamp_seconds",
        float("inf"),  # type: ignore
    )

    org_should_use_items_attrs = state.get_config(
        f"use_eap_items_attrs_table__{request_meta.organization_id}", False
    )

    all_should_use_items_attrs = state.get_config(
        "use_eap_items_attrs_table_all", False
    )

    if (
        org_should_use_items_attrs or all_should_use_items_attrs
    ) and use_eap_items_attrs_table_start_timestamp_seconds is not None:
        return (
            request_meta.start_timestamp.seconds or 0
        ) >= use_eap_items_attrs_table_start_timestamp_seconds

    return False
