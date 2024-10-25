from snuba.query.query_settings import HTTPQuerySettings


def setup_trace_query_settings(enable_trace_logs: bool) -> HTTPQuerySettings:
    query_settings = HTTPQuerySettings()
    if enable_trace_logs:
        query_settings.set_clickhouse_settings(
            {"send_logs_level": "trace", "log_profile_events": 1}
        )
    return query_settings
