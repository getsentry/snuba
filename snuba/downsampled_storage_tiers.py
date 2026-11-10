from __future__ import annotations

from enum import IntEnum


class Tier(IntEnum):
    TIER_NO_TIER = -1
    TIER_1 = 1
    TIER_8 = 8
    TIER_64 = 64
    TIER_512 = 512

    @classmethod
    def valid_tiers(cls) -> list["Tier"]:  # noqa: UP037
        return [tier for tier in cls if tier > 0]

    def next_coarser(self) -> Tier:
        ladder = self.valid_tiers()
        try:
            idx = ladder.index(self)
        except ValueError:
            return ladder[0]
        return ladder[min(idx + 1, len(ladder) - 1)]
