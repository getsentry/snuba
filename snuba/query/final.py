from snuba.state.sentry_options import get_option

DISABLE_QUERY_FINAL_OPTION = "disable_query_final"


def query_final_disabled() -> bool:
    """Return True when the FINAL killswitch is on.

    Applies to errors (PostReplacementConsistencyEnforcer and replacer SELECTs) and
    EAP get_trace. It does not apply to ConsistencyEnforcerProcessor storages
    (group_attributes, groupedmessage, groupassignee), which always need FINAL.
    """
    return get_option(DISABLE_QUERY_FINAL_OPTION, False)
