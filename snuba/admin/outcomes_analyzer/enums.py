from __future__ import annotations

from enum import IntEnum
from typing import TypedDict

from sentry_relay.consts import DataCategory


class Outcome(IntEnum):
    """
    Outcomes written to the outcomes dataset.

    Mirrors sentry.utils.outcomes.Outcome / Relay producer values. Kept here so
    snuba-admin can expose the list without depending on the sentry package.
    """

    ACCEPTED = 0
    FILTERED = 1
    RATE_LIMITED = 2
    INVALID = 3
    ABUSE = 4
    CLIENT_DISCARD = 5
    CARDINALITY_LIMITED = 6


class EnumOption(TypedDict):
    value: str
    label: str


def _option(value: int, name: str) -> EnumOption:
    return {"value": str(value), "label": f"{value} — {name.lower()}"}


def data_category_options() -> list[EnumOption]:
    """Relay DataCategory members for outcomes investigation dropdowns."""
    options: list[EnumOption] = []
    for member in DataCategory:
        if member is DataCategory.UNKNOWN:
            continue
        options.append(_option(int(member), member.name))
    options.sort(key=lambda item: int(item["value"]))
    return options


def outcome_options() -> list[EnumOption]:
    """Outcome enum members for outcomes investigation dropdowns."""
    return [_option(int(member), member.name) for member in Outcome]


def outcomes_enum_options() -> dict[str, list[EnumOption]]:
    return {
        "categories": data_category_options(),
        "outcomes": outcome_options(),
    }
