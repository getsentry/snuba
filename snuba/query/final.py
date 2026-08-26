from snuba.state.sentry_options import get_option

DISABLE_QUERY_FINAL_OPTION = "disable_query_final"


def query_final_disabled() -> bool:
    """Return True when the global FINAL killswitch is on."""
    return get_option(DISABLE_QUERY_FINAL_OPTION, False)
