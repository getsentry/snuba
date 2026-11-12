from typing import Any
from unittest.mock import patch

import pytest
from sentry_options.testing import override_options

from snuba.state.retention import enforce_retention_days, get_retention_days_config


def test_schema_default() -> None:
    config = get_retention_days_config()
    assert config["standard"] == {"default": 30, "max": 396}
    assert config["downsampled"] == {"default": 396, "max": 396}


def test_missing_option_falls_back_to_schema_defaults() -> None:
    with patch("snuba.state.retention.get_option", return_value={}):
        assert get_retention_days_config() == {
            "standard": {"default": 30, "max": 396},
            "downsampled": {"default": 396, "max": 396},
        }
        assert enforce_retention_days(None) == 30
        assert enforce_retention_days(396) == 396


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (None, "standard", 30),
        (0, "standard", 30),
        (-1, "standard", 30),
        (7, "standard", 7),
        (30, "standard", 30),
        (60, "standard", 60),
        (90, "standard", 90),
        (180, "standard", 180),
        (366, "standard", 366),
        (396, "standard", 396),
        # anything else is rejected, never rounded
        (1, "standard", None),
        (14, "standard", None),
        (29, "standard", None),
        (31, "standard", None),
        (89, "standard", None),
        (100, "standard", None),
        (365, "standard", None),
        (400, "standard", None),
        (None, "downsampled", 396),
        (90, "downsampled", 90),
        (366, "downsampled", 366),
        (396, "downsampled", 396),
        (365, "downsampled", None),
        (420, "downsampled", None),
    ],
)
def test_enforce_retention_days(value: int | None, kind: str, expected: int | None) -> None:
    assert enforce_retention_days(value, kind) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (None, "standard", 60),
        (90, "standard", 90),
        (180, "standard", 180),
        # supported but above max: rejected, not capped
        (366, "standard", None),
        (396, "standard", None),
        (None, "downsampled", 180),
        (366, "downsampled", 366),
        (396, "downsampled", None),
    ],
)
def test_values_above_max_are_rejected(value: int | None, kind: str, expected: int | None) -> None:
    override: dict[str, Any] = {
        "retention_days": {
            "standard": {"default": 60, "max": 180},
            "downsampled": {"default": 180, "max": 366},
        }
    }
    with override_options("snuba", override):
        assert enforce_retention_days(value, kind) == expected  # type: ignore[arg-type]
