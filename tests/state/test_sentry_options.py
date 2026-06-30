from unittest import mock

from sentry_options.testing import override_options

from snuba.state.sentry_options import (
    SNUBA_OPTIONS_NAMESPACE,
    get_mapped_option,
    get_option,
)


def test_get_option_returns_schema_default() -> None:
    # `enable_any_attribute_filter` has a schema default of `true`. With
    # sentry-options initialized (see tests/conftest.py) we read that schema
    # default rather than the fallback passed here.
    assert get_option("enable_any_attribute_filter", False) is True


def test_get_option_returns_value_at_its_schema_type() -> None:
    # No coercion: each value comes back as its declared schema type.
    assert get_option("default_tier", 0) == 1
    assert get_option("rpc_logging_sample_rate", 1.0) == 0.0
    assert get_option("ExecutionStage.disable_max_query_size_check_for_clusters", "x") == ""


def test_override_options_changes_value() -> None:
    with override_options(SNUBA_OPTIONS_NAMESPACE, {"enable_any_attribute_filter": False}):
        assert get_option("enable_any_attribute_filter", True) is False
    # the override is scoped to the context manager; the default is restored.
    assert get_option("enable_any_attribute_filter", False) is True


def test_unknown_option_falls_back_to_default() -> None:
    # Keys absent from the schema raise UnknownOptionError internally, which
    # get_option swallows in favor of the caller-supplied default.
    assert get_option("option_that_does_not_exist", "fallback") == "fallback"


def test_unexpected_error_falls_back_to_default() -> None:
    # The client should only ever raise OptionsError, but a non-OptionsError
    # escaping from the client must not crash hot query paths: get_option honors
    # the "any reason" fallback contract.
    with mock.patch(
        "snuba.state.sentry_options.sentry_options.options",
        side_effect=RuntimeError("boom"),
    ):
        assert get_option("enable_any_attribute_filter", "fallback") == "fallback"


def test_mapped_option_returns_entry_for_name() -> None:
    # A dict-typed option (additionalProperties) keyed by the dynamic name.
    with override_options(
        SNUBA_OPTIONS_NAMESPACE,
        {"lw_deletes_split_by_partition": {"search_issues": 1, "errors": 0}},
    ):
        assert get_mapped_option("lw_deletes_split_by_partition", "search_issues", 9) == 1
        assert get_mapped_option("lw_deletes_split_by_partition", "errors", 9) == 0


def test_mapped_option_returns_entry_at_schema_type() -> None:
    # No coercion: a mapped entry is returned exactly as stored.
    with override_options(
        SNUBA_OPTIONS_NAMESPACE,
        {
            "snql_disabled_dataset": {"events": True},
            "validate_schema_sample_rate": {"events": 0.25},
        },
    ):
        assert get_mapped_option("snql_disabled_dataset", "events", False) is True
        assert get_mapped_option("validate_schema_sample_rate", "events", 1.0) == 0.25


def test_mapped_option_falls_back_for_absent_name() -> None:
    with override_options(
        SNUBA_OPTIONS_NAMESPACE,
        {"lw_deletes_killswitch": {"search_issues": "[1]"}},
    ):
        assert get_mapped_option("lw_deletes_killswitch", "search_issues", "") == "[1]"
        # A name with no entry falls back to the caller default.
        assert get_mapped_option("lw_deletes_killswitch", "transactions", "x") == "x"


def test_mapped_option_falls_back_when_option_unset() -> None:
    # Each dict option defaults to {} (empty), so every name falls back to the
    # caller-supplied default — preserving the pre-migration per-key default.
    assert get_mapped_option("lw_deletes_split_by_partition", "search_issues", 7) == 7
    assert get_mapped_option("validate_schema_sample_rate", "events", 1.0) == 1.0


def test_mapped_option_unknown_option_falls_back() -> None:
    # An option absent from the schema falls back to the caller default.
    assert get_mapped_option("option_that_does_not_exist", "x", "fallback") == "fallback"
