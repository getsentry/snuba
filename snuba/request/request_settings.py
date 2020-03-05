from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from typing import Any, Mapping, Optional, Sequence

from snuba.state.rate_limit import get_global_rate_limit_params, RateLimitParameters


class SamplingMode(Enum):
    """
    The value of this ennum are part of the public schema. Do not change them
    without ensuring backward compatibility.
    """

    # No sampling applied.
    NONE = "none"
    # A fixed 0 - 1.0 (both exclusive) sampling rate is provided.
    FIXED = "fixed"
    # Adaptive sampling rate with aggregation adjustment. In this mode Snuba
    # tries to guess an appropriate sampling rate depending on several heuristics
    # including stats on the expected amount of data to query.
    # Snuba will try its best to adjust the result of aggregations according to
    # the sampling rate where it makes sense. If an aggregation function is passed
    # that Snuba does not know how to adjust, Snuba will bail and apply no sampling
    # so the client will still receive data that does not need adjustment, thus
    # copmliant with what was requested.
    ADAPTIVE_ADJUSTED = "adaptive_adjusted"
    # Adaptive sampling rate without aggregation adjustment. It works like for
    # adaptive_adjusted but Snuba will not apply any adjustment. It is responsibility
    # of the client to apply the appropriate adjustment.
    # This gives higher guarantees that sampling will actually be applied since
    # Snuba does not need to know how to adjust the aggregations provided in the
    # query.
    ADAPTIVE_UNADJUSTED = "adaptive_unadjusted"
    # The old automatic approach that applies a fixed config sampling if in TURBO
    # mode.
    AUTO_DEPRECATED = "auto"


@dataclass(frozen=True)
class SamplingConfig:
    mode: SamplingMode
    parameters: Mapping[str, Any]

    @classmethod
    def build_from_dict(cls, raw_config: Mapping[str, Any]) -> SamplingConfig:
        mode = raw_config["mode"]

        rate = raw_config.get("rate")
        if mode != SamplingMode.FIXED:
            assert rate is None, f"Sampling mode {mode} does not support a fixed rate"
            params = {}
        else:
            assert rate is not None, f"Sampling mode {mode} requires a fixed rate"
            params = {"rate": rate}
        return SamplingConfig(SamplingMode(mode), params)

    @classmethod
    def build_default(cls) -> SamplingConfig:
        return SamplingConfig(SamplingMode.AUTO_DEPRECATED, None)


class RequestSettings(ABC):
    """
    Settings that apply to how the query in the request should be run.

    The settings provided in this class do not directly affect the SQL statement that will be created
    (i.e. they do not directly appear in the SQL statement).

    They can indirectly affect the SQL statement that will be formed. For example, `turbo` affects
    the formation of the query for projects, but it doesn't appear in the SQL statement.
    """

    @abstractmethod
    def get_turbo(self) -> bool:
        pass

    @abstractmethod
    def get_consistent(self) -> bool:
        pass

    @abstractmethod
    def get_debug(self) -> bool:
        pass

    @abstractmethod
    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        pass

    @abstractmethod
    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass

    @abstractmethod
    def get_sampling_config(self) -> SamplingConfig:
        pass


class HTTPRequestSettings(RequestSettings):
    """
    Settings that are applied to all Requests initiated via the HTTP api. Allows
    parameters to be customized, defaults to using global rate limits and allows
    additional rate limits to be added.
    """

    def __init__(
        self,
        turbo: bool = False,
        consistent: bool = False,
        debug: bool = False,
        sampling_config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.__turbo = turbo
        self.__consistent = consistent
        self.__debug = debug
        self.__rate_limit_params = [get_global_rate_limit_params()]
        self.__sampling_config = (
            SamplingConfig.build_from_dict(sampling_config)
            if sampling_config
            else SamplingConfig.build_default()
        )

        # TODO: Enforce we no other sampling rate except for None can be used
        # if we are not in turbo mode
        if self.__sampling_config.mode not in (
            SamplingMode.NONE,
            SamplingMode.AUTO_DEPRECATED,
        ):
            assert (
                self.get_turbo()
            ), "Cannot use adaptive sampling config if not in turbo mode."

    def get_turbo(self) -> bool:
        return self.__turbo

    def get_consistent(self) -> bool:
        return self.__consistent

    def get_debug(self) -> bool:
        return self.__debug

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return self.__rate_limit_params

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        self.__rate_limit_params.append(rate_limit_param)

    def get_sampling_config(self) -> SamplingConfig:
        return self.__sampling_config


class SubscriptionRequestSettings(RequestSettings):
    """
    Settings that are applied to Requests initiated via Subscriptions. Hard code most
    parameters and skips all rate limiting.
    """

    def get_turbo(self) -> bool:
        return False

    def get_consistent(self) -> bool:
        return True

    def get_debug(self) -> bool:
        return False

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return []

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass

    def get_sampling_config(self) -> SamplingConfig:
        return SamplingConfig.build_default()
