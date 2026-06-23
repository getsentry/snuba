from sentry_options.testing import override_options

from snuba.state.sentry_options import SNUBA_OPTIONS_NAMESPACE, get_option


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
