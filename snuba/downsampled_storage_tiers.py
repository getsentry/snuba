from __future__ import annotations

from enum import IntEnum


class Tier(IntEnum):
    TIER_NO_TIER = -1
    TIER_1 = 1
    TIER_8 = 8
    TIER_64 = 64
    TIER_512 = 512

    @classmethod
    def valid_tiers(cls) -> list[Tier]:
        return [tier for tier in cls if tier > 0]

    def lower(self) -> Tier:
        ladder = self.valid_tiers()
        if self not in ladder:
            return Tier.TIER_1
        max_idx = len(ladder) - 1
        self_idx = ladder.index(self)
        next_idx = min(self_idx + 1, max_idx)
        
        return ladder[next_idx]
