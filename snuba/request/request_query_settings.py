from typing import Sequence

from snuba.state.rate_limit import get_global_rate_limit_params, RateLimitParameters


class RequestQuerySettings:
    def __init__(self, turbo: bool, consistent: bool, debug: bool):
        self.__turbo = turbo
        self.__consistent = consistent
        self.__debug = debug
        self.__rate_limit_params = [get_global_rate_limit_params()]

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
