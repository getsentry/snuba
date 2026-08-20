from typing import Any

import pytest
from sentry_options.testing import override_options

from snuba.state.retention import (
    DEFAULT_RETENTION_DAYS,
    get_retention_days_config,
    quantize_retention_days,
)


def test_schema_default_is_quantized() -> None:
    config = get_retention_days_config()
    assert config == DEFAULT_RETENTION_DAYS
    assert config["standard"] == {"default": 30, "max": 90}
    assert config["downsampled"] == {"default": 390, "max": 390}


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (None, "standard", 90),
        (0, "standard", 90),
        (-1, "standard", 90),
        (29, "standard", 30),
        (30, "standard", 30),
        (31, "standard", 30),
        (60, "standard", 60),
        (89, "standard", 60),
        (90, "standard", 90),
        (100, "standard", 90),
        (120, "standard", 90),
        (None, "downsampled", 390),
        (365, "downsampled", 360),
        (396, "downsampled", 390),
        (420, "downsampled", 390),
    ],
)
def test_quantize_retention_days(value: int | None, kind: str, expected: int) -> None:
    assert quantize_retention_days(value, kind) == expected  # type: ignore[arg-type]


def test_quantize_honors_option_override() -> None:
    override: dict[str, Any] = {
        "retention_days": {
            "standard": {"default": 60, "max": 180},
            "downsampled": {"default": 180, "max": 360},
        }
    }
    with override_options("snuba", override):
        assert quantize_retention_days(None) == 180
        assert quantize_retention_days(100) == 90
        assert quantize_retention_days(179) == 150
        assert quantize_retention_days(200) == 180
        assert quantize_retention_days(365, "downsampled") == 360
        assert quantize_retention_days(None, "downsampled") == 360
