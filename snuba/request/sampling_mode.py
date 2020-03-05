from __future__ import annotations

from typing import Any, Mapping, Type

from dataclasses import dataclass
from abc import ABC, abstractclassmethod


class SamplingConfig(ABC):
    @abstractclassmethod
    def get_mode(cls) -> str:
        raise NotImplementedError


class NoSamplingConfig(SamplingConfig):
    """
    No sampling applied
    """

    @classmethod
    def get_mode(cls) -> str:
        return "none"


@dataclass(frozen=True)
class FixedSamplingConfig(SamplingConfig):
    """
    A fixed 0 - 1.0 (both exclusive) sampling rate is provided.
    """

    rate: float

    @classmethod
    def get_mode(cls) -> str:
        return "fixed"


class AdjustedSamplingConfig(SamplingConfig):
    """
    Adaptive sampling rate with aggregation adjustment. In this mode Snuba
    tries to guess an appropriate sampling rate depending on several heuristics
    including stats on the expected amount of data to query.
    Snuba will try its best to adjust the result of aggregations according to
    the sampling rate where it makes sense. If an aggregation function is passed
    that Snuba does not know how to adjust, Snuba will bail and apply no sampling
    so the client will still receive data that does not need adjustment, thus
    copmliant with what was requested.
    """

    @classmethod
    def get_mode(cls) -> str:
        return "adaptive_adjusted"


class UnadjustedSamplingConfig(SamplingConfig):
    """
    Adaptive sampling rate without aggregation adjustment. It works like for
    adaptive_adjusted but Snuba will not apply any adjustment. It is responsibility
    of the client to apply the appropriate adjustment.
    This gives higher guarantees that sampling will actually be applied since
    Snuba does not need to know how to adjust the aggregations provided in the
    query.
    """

    @classmethod
    def get_mode(cls) -> str:
        return "adaptive_unadjusted"


class AutoSamplingConfig(SamplingConfig):
    """
    The old automatic approach that applies a fixed config sampling if in TURBO
    mode.
    """

    @classmethod
    def get_mode(cls) -> str:
        return "auto"


MODES_MAPPING: Mapping[str, Type[SamplingConfig]] = {
    c.get_mode(): c
    for c in [
        NoSamplingConfig,
        FixedSamplingConfig,
        AdjustedSamplingConfig,
        UnadjustedSamplingConfig,
        AutoSamplingConfig,
    ]
}


def build_sampling_config(raw_config: Mapping[str, Any]) -> SamplingConfig:
    mode = raw_config["mode"]
    rate = raw_config.get("rate")

    if mode != FixedSamplingConfig.get_mode():
        assert rate is None, f"Sampling mode {mode} does not support a fixed rate"
        return MODES_MAPPING[mode]()
    else:
        assert rate is not None, f"Sampling mode {mode} requires a fixed rate"
        return FixedSamplingConfig(rate)
