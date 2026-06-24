from unittest import mock

from sentry_options.testing import override_options

from snuba.state.sentry_options import (
    SNUBA_OPTIONS_NAMESPACE,
    get_bool_option,
    get_float_option,
    get_int_option,
    get_option,
    get_str_option,
)


def test_get_option_returns_schema_default() -> None:
    # `enable_any_attribute_filter` has a schema default of `true`. With
    # sentry-options initialized (see tests/conftest.py) we read that schema
    # default rather than the fallback passed here.
    assert get_option("enable_any_attribute_filter", False) is True


def test_override_options_changes_value() -> None:
    with override_options(SNUBA_OPTIONS_NAMESPACE, {"enable_any_attribute_filter": False}):
        assert get_option("enable_any_attribute_filter", True) is False
    # the override is scoped to the context manager; the default is restored.
    assert get_option("enable_any_attribute_filter", False) is True


def test_unknown_option_falls_back_to_default() -> None:
    # Keys absent from the schema raise UnknownOptionError internally, which
    # get_option swallows in favor of the caller-supplied default.
    assert get_option("option_that_does_not_exist", "fallback") == "fallback"


def test_typed_accessors_return_schema_defaults() -> None:
    # Each typed accessor returns the schema default with the right Python type.
    assert get_bool_option("aggregation_deprecation_enabled", False) is True
    assert get_int_option("default_tier", 0) == 1
    assert get_int_option("export_trace_items_default_page_size", 0) == 10000
    assert get_float_option("rpc_logging_sample_rate", 1.0) == 0.0
    assert get_str_option("ExecutionStage.disable_max_query_size_check_for_clusters", "x") == ""


def test_typed_accessors_honor_overrides() -> None:
    with override_options(
        SNUBA_OPTIONS_NAMESPACE,
        {
            "aggregation_deprecation_enabled": False,
            "default_tier": 8,
            "rpc_logging_sample_rate": 0.25,
        },
    ):
        assert get_bool_option("aggregation_deprecation_enabled", True) is False
        assert get_int_option("default_tier", 1) == 8
        assert get_float_option("rpc_logging_sample_rate", 0.0) == 0.25


def test_typed_accessors_fall_back_on_unknown_option() -> None:
    # Unknown keys fall back to the caller-supplied default at the right type.
    assert get_bool_option("missing_bool", True) is True
    assert get_int_option("missing_int", 7) == 7
    assert get_float_option("missing_float", 1.5) == 1.5
    assert get_str_option("missing_str", "fallback") == "fallback"


def test_unexpected_error_falls_back_to_default() -> None:
    # The client should only ever raise OptionsError, but a non-OptionsError
    # escaping from the client must not crash hot query paths: get_option (and
    # the typed accessors built on it) honor the "any reason" fallback contract.
    with mock.patch(
        "snuba.state.sentry_options.sentry_options.options",
        side_effect=RuntimeError("boom"),
    ):
        assert get_option("enable_any_attribute_filter", "fallback") == "fallback"
        assert get_bool_option("enable_any_attribute_filter", True) is True
        assert get_int_option("default_tier", 7) == 7
        assert get_float_option("rpc_logging_sample_rate", 1.5) == 1.5
        assert get_str_option("some_str", "fallback") == "fallback"
