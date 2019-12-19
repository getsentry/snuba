from typing import Sequence

from snuba.state.rate_limit import get_global_rate_limit_params, RateLimitParameters


class RequestSettings:
    """
    Settings that apply to how the query in the request should be run.

    The settings provided in this class do not directly affect the SQL statement that will be created
    (i.e. they do not directly appear in the SQL statement).

    They can indirectly affect the SQL statement that will be formed. For example, `turbo` affects
    the formation of the query for projects, but it doesn't appear in the SQL statement.
    """

    def __init__(self, turbo: bool, consistent: bool, debug: bool) -> None:
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


class SubscriptionRequestSettings(RequestSettings):
    def __init__(self, turbo: bool, consistent: bool, debug: bool) -> None:
        self.__debug = debug

    def get_turbo(self) -> bool:
        return False

    def get_consistent(self) -> bool:
        return True

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return []

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass
