from typing import Any
from unittest.mock import patch

import pytest
from sentry_options.testing import override_options

from snuba.state.retention import clamp_retention_days, get_retention_days_config


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
