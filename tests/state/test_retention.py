from typing import Any
from unittest.mock import patch

import pytest
from sentry_options.testing import override_options

from snuba.state.retention import (
    bucket_retention_days,
    clamp_retention_days,
    enforce_standard_retention_days,
    get_retention_days_config,
    query_standard_retention_days,
)


def test_schema_default() -> None:
    config = get_retention_days_config()
    assert config["standard"] == {"default": 30, "max": 90}
    assert config["downsampled"] == {"default": 396, "max": 396}


def test_missing_option_falls_back_to_schema_defaults() -> None:
    with patch("snuba.state.retention.get_option", return_value={}):
        assert get_retention_days_config() == {
            "standard": {"default": 30, "max": 90},
            "downsampled": {"default": 396, "max": 396},
        }
        assert clamp_retention_days(None) == 30
        assert clamp_retention_days(100) == 90
        assert enforce_standard_retention_days(None) == 30
        assert enforce_standard_retention_days(100) == 90


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (None, "standard", 30),
        (0, "standard", 30),
        (-1, "standard", 30),
        (29, "standard", 29),
        (30, "standard", 30),
        (31, "standard", 31),
        (60, "standard", 60),
        (89, "standard", 89),
        (90, "standard", 90),
        (100, "standard", 90),
        (120, "standard", 90),
        (None, "downsampled", 396),
        (365, "downsampled", 365),
        (396, "downsampled", 396),
        (420, "downsampled", 396),
    ],
)
def test_clamp_retention_days(value: int | None, kind: str, expected: int) -> None:
    assert clamp_retention_days(value, kind) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (None, "standard", 60),
        (100, "standard", 100),
        (179, "standard", 179),
        (200, "standard", 180),
        (None, "downsampled", 180),
        (365, "downsampled", 360),
    ],
)
def test_clamp_honors_option_override(value: int | None, kind: str, expected: int) -> None:
    override: dict[str, Any] = {
        "retention_days": {
            "standard": {"default": 60, "max": 180},
            "downsampled": {"default": 180, "max": 360},
        }
    }
    with override_options("snuba", override):
        assert clamp_retention_days(value, kind) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "max_days", "expected"),
    [
        (7, 366, 7),
        (1, 366, None),
        (8, 366, None),
        (14, 366, None),
        (29, 366, None),
        (30, 366, 30),
        (45, 366, 30),
        (60, 366, 60),
        (89, 366, 60),
        (90, 366, 90),
        (179, 366, 90),
        (180, 366, 180),
        (365, 366, 180),
        (366, 366, 366),
        (396, 366, 366),
        (1000, 366, 366),
        (366, 90, 90),
        (366, 120, 90),
    ],
)
def test_bucket_retention_days(value: int, max_days: int, expected: int | None) -> None:
    assert bucket_retention_days(value, max_days) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, 30),
        (0, 30),
        (7, 7),
        (14, None),
        (30, 30),
        (89, 60),
        (90, 90),
        (365, 180),
        (366, 366),
        (400, 366),
    ],
)
def test_enforce_standard_retention_days(value: int | None, expected: int | None) -> None:
    override: dict[str, Any] = {
        "retention_days": {
            "standard": {"default": 30, "max": 366},
            "downsampled": {"default": 396, "max": 396},
        }
    }
    with override_options("snuba", override):
        assert enforce_standard_retention_days(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, 30),
        (0, 30),
        (7, 7),
        # Unsupported values below 30 are never written; use them unchanged.
        (14, 14),
        (89, 60),
        (365, 180),
        (400, 366),
    ],
)
def test_query_standard_retention_days(value: int | None, expected: int) -> None:
    override: dict[str, Any] = {
        "retention_days": {
            "standard": {"default": 30, "max": 366},
            "downsampled": {"default": 396, "max": 396},
        }
    }
    with override_options("snuba", override):
        assert query_standard_retention_days(value) == expected
