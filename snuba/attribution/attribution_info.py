from __future__ import annotations

from dataclasses import dataclass

from snuba.attribution import AppID


@dataclass(frozen=True)
class AttributionInfo:
    """The settings for a attribution of a query + quota enforcement
    should be immutable
    """

    app_id: AppID
    referrer: str
    team: str | None
    feature: str | None
    parent_api: str | None
